"""Deterministic subscription-delivery replay on top of the controlled replication model.

Pure standard library so the engine itself is unit-testable without the web stack.
Every committed write gets a monotonic change sequence inside the named lineage.
A subscriber may be fed by the primary, or by a healthy same-lineage replica whose
applied position already contains the change. Acks confirm in strict order: the
subscription cursor never jumps over an unconfirmed change, even when acks arrive
out of order. Retries are idempotent on the subscriber side.

The simulation is a discrete event loop over a single timer queue. Input events
and virtual timers (delivery, ack-timeout, reconcile probe) are merged on one
timeline, so write / replication / failover / ack ordering is fully observable.
"""
import copy
import hashlib
import json

ACK_POLICIES = ('resend', 'reconcile')
INPUT_EVENT_KINDS = ('write', 'replicate', 'node_down', 'node_up',
                     'subscribe', 'sub_down', 'sub_up', 'ack')
MAX_RETRIES = 8  # bounded retransmits keep the deterministic replay convergent


def _validate_inputs(nodes, events, delivery_ms, ack_timeout_ms,
                     reconcile_rtt_ms, inflight_window, policy):
    if not 1 <= len(nodes) <= 12:
        raise ValueError('需要1–12个节点')
    if not 1 <= len(events) <= 100:
        raise ValueError('需要1–100个时序事件')
    if policy not in ACK_POLICIES:
        raise ValueError('回执策略必须是 resend 或 reconcile')
    for name, bound in (('delivery_ms', delivery_ms), ('ack_timeout_ms', ack_timeout_ms),
                        ('reconcile_rtt_ms', reconcile_rtt_ms)):
        if not isinstance(bound, int) or bound < 0:
            raise ValueError(name + '不得为负整数')
    if not isinstance(inflight_window, int) or inflight_window < 1:
        raise ValueError('在途窗口必须是不小于1的整数')
    if len({x['name'] for x in nodes}) != len(nodes):
        raise ValueError('节点名必须唯一')
    primaries = [x for x in nodes if x['role'] == 'primary']
    if len(primaries) != 1:
        raise ValueError('必须且只能配置一个主节点')
    last_time = -1
    for event in events:
        at = event['at_ms']
        if at < last_time:
            raise ValueError('事件必须按时间非递减排列')
        last_time = at
        kind = event['kind']
        if kind not in INPUT_EVENT_KINDS:
            raise ValueError('未知事件类型：' + str(kind))
        if kind in ('replicate', 'node_down', 'node_up') and not event.get('node'):
            raise ValueError(kind + ' 事件需要 node')
        if kind == 'replicate':
            if event.get('position') is None or event.get('delay_ms') is None \
                    or event['delay_ms'] < 0:
                raise ValueError('replicate 需要 position 和非负 delay_ms')
        if kind in ('subscribe', 'sub_down', 'sub_up', 'ack') and not event.get('subscriber'):
            raise ValueError(kind + ' 事件需要 subscriber')
        if kind == 'ack' and event.get('seq') is None:
            raise ValueError('ack 事件需要 seq')


def simulate(node_inputs, events, policy='resend', delivery_ms=120,
             ack_timeout_ms=400, reconcile_rtt_ms=300, inflight_window=1,
             auto_reack=True):
    """Replay the scenario and return a deterministic evidence document."""
    _validate_inputs(node_inputs, events, delivery_ms, ack_timeout_ms,
                     reconcile_rtt_ms, inflight_window, policy)
    nodes = {x['name']: copy.deepcopy(x) for x in node_inputs}
    primary_name = next(x['name'] for x in node_inputs if x['role'] == 'primary')
    primary = nodes[primary_name]
    lineage = primary['lineage']
    if any(n['role'] == 'replica' and n['lineage'] == lineage
           and n['position'] > primary['position'] for n in nodes.values()):
        raise ValueError('同日志谱系副本位点不得高于主库初始位点')

    changes = {}                      # seq -> change record
    next_seq = 1
    subscribers = {}                  # name -> state
    timers = []                       # list of dict timers, kept sorted by pop
    timer_seq = [0]                   # monotonic tie-breaker for stable ordering
    timeline = []

    def log(kind, at_ms, **fields):
        timeline.append({'kind': kind, 'at_ms': at_ms, **fields})

    def push(at_ms, kind, sub, seq, extra):
        timer_seq[0] += 1
        timers.append({'at': at_ms, 'kind': kind, 'sub': sub, 'seq': seq,
                       'extra': extra, 'order': timer_seq[0]})
        timers.sort(key=lambda t: (t['at'], t['order']))

    def cancel(kind, sub, seq):
        timers[:] = [t for t in timers
                     if not (t['kind'] == kind and t['sub'] == sub and t['seq'] == seq)]

    def source_for(seq):
        change = changes[seq]
        if primary['status'] == 'healthy' and primary['lineage'] == lineage \
                and primary['position'] >= change['position']:
            return primary_name, '主库在线且持有该变更，主库供数'
        candidates = [n for n in nodes.values()
                      if n['role'] == 'replica' and n['status'] == 'healthy'
                      and n['lineage'] == lineage
                      and n['position'] >= change['position']]
        candidates.sort(key=lambda n: (n['delay_ms'], -n['position'], n['name']))
        if candidates:
            return candidates[0]['name'], '主库不可用，已追平位点的副本继续供数'
        return None, '主库与同谱系达标副本均不可用，缺位点等待'

    def schedule_delivery(sub, seq, now_ms, reason, attempt):
        """Return True when a delivery timer is (or remains) scheduled."""
        if seq in sub['inflight']:
            return True
        if any(t['sub'] == sub['name'] and t['seq'] == seq
               and t['kind'] in ('deliver', 'probe_send', 'probe_reply') for t in timers):
            return True
        source, why = source_for(seq)
        if source is None:
            sub['blocked'] = {'seq': seq, 'reason': why}
            return False
        if sub['status'] != 'healthy':
            sub['blocked'] = {'seq': seq, 'reason': '订阅方连接中断，恢复后重试'}
            return False
        if len(sub['inflight']) >= inflight_window:
            sub['blocked'] = {'seq': seq, 'reason': '在途窗口已满，等待最早未确认变更回执'}
            return False
        sub['blocked'] = None
        sub['inflight'][seq] = {'attempt': attempt, 'source': source}
        change = changes[seq]
        push(now_ms + delivery_ms, 'deliver', sub['name'], seq,
             {'attempt': attempt, 'source': source, 'reason': reason,
              'key': change['key'], 'value': change['value'], 'why': why})
        return True

    def advance(sub, now_ms):
        """Schedule every change the subscriber may ask for, filling the window."""
        seq = sub['acked'] + 1
        while seq in changes:
            if seq in sub['inflight']:
                seq += 1  # already in flight; try the next seq within the window
                continue
            if any(t['sub'] == sub['name'] and t['seq'] == seq
                   and t['kind'] in ('deliver', 'probe_send', 'probe_reply')
                   for t in timers):
                seq += 1
                continue
            attempt = sub['inflight'].get(seq, {}).get('attempt', 0) + 1
            if not schedule_delivery(sub, seq, now_ms, '正常投递', attempt):
                return  # blocked source, down subscriber, or full window
            seq += 1

    def do_deliver(t, now_ms):
        name, seq, extra = t['sub'], t['seq'], t['extra']
        sub = subscribers[name]
        change = changes[seq]
        source_now, _ = source_for(seq)
        if source_now is None or nodes[extra['source']]['status'] != 'healthy':
            # The chosen source failed while the delivery was in flight: drop it
            # and re-arm through whichever node now holds the position.
            log('deliver', now_ms, subscriber=name, seq=seq,
                source=extra['source'], attempt=extra['attempt'],
                key=change['key'], value=change['value'],
                arrived=False, duplicate=False, applied=False,
                reason='供数节点在投递途中下线，该投丢弃，改由持有位点的节点重投')
            sub['inflight'].pop(seq, None)
            schedule_delivery(sub, seq, now_ms, '故障转移重投', extra['attempt'] + 1)
            return
        if sub['status'] != 'healthy':
            log('deliver', now_ms, subscriber=name, seq=seq,
                source=extra['source'], attempt=extra['attempt'],
                key=change['key'], value=change['value'],
                arrived=False, duplicate=False, applied=False,
                reason='订阅方连接中断，投递未到达，等待恢复后从未确认位点重试')
            sub['inflight'].pop(seq, None)
            sub['blocked'] = {'seq': seq,
                              'reason': '订阅方连接中断，未确认变更 #%d 挂起，恢复后重试' % seq}
            push(now_ms + ack_timeout_ms, 'ack_timeout', name, seq,
                 {'attempt': extra['attempt'], 'source': extra['source']})
            return
        already = seq <= sub['delivered']
        if already:
            sub['duplicates'] += 1
        else:
            sub['delivered'] = max(sub['delivered'], seq)
            sub['applied'] = max(sub['applied'], seq)
        sub['received'].add(seq)
        log('deliver', now_ms, subscriber=name, seq=seq,
            source=extra['source'], attempt=extra['attempt'],
            key=change['key'], value=change['value'],
            arrived=True, duplicate=already, applied=not already,
            reason=('重复收件：回执丢失后的重发，按seq幂等去重，不重复入账'
                    if already else extra.get('why', extra.get('reason', '正常投递'))))
        if already:
            # Idempotent subscriber recognizes the retry and re-issues its ack.
            if auto_reack:
                push(now_ms, 'vack', name, seq, {})
        push(now_ms + ack_timeout_ms, 'ack_timeout', name, seq,
             {'attempt': extra['attempt'], 'source': extra['source']})

    def do_timeout(t, now_ms):
        name, seq, extra = t['sub'], t['seq'], t['extra']
        sub = subscribers[name]
        if seq not in sub['inflight'] and not any(
                x['sub'] == name and x['seq'] == seq and x['kind'] == 'deliver'
                for x in timers):
            return
        if seq <= sub['acked']:
            sub['inflight'].pop(seq, None)
            return
        attempt = extra['attempt']
        if attempt >= MAX_RETRIES:
            sub['blocked'] = {'seq': seq,
                              'reason': '重发次数已达上限（%d），保留未确认状态等待人工介入' % MAX_RETRIES}
            sub['inflight'].pop(seq, None)
            log('ack_timeout', now_ms, subscriber=name, seq=seq,
                source=extra['source'], attempt=attempt,
                reason='重发次数达上限，停止自动重发，订阅进度停在未确认位点')
            return
        if policy == 'reconcile':
            log('probe_send', now_ms, subscriber=name, seq=seq,
                source=extra['source'], attempt=attempt,
                reason='回执超时，先查询订阅方已收位点，不盲目重发')
            push(now_ms + reconcile_rtt_ms, 'probe_reply', name, seq,
                 {'attempt': attempt, 'source': extra['source']})
            return
        log('ack_timeout', now_ms, subscriber=name, seq=seq,
            source=extra['source'], attempt=attempt,
            reason='回执超时（判定丢失），直接重发该变更')
        sub['inflight'].pop(seq, None)
        schedule_delivery(sub, seq, now_ms, '超时重发', attempt + 1)

    def do_probe_reply(t, now_ms):
        name, seq, extra = t['sub'], t['seq'], t['extra']
        sub = subscribers[name]
        has = seq <= sub['delivered']
        log('probe_reply', now_ms, subscriber=name, seq=seq,
            source=extra['source'], attempt=extra['attempt'],
            reason=('查询结果：订阅方已入账该seq，仅补登回执即可，不重复收件'
                    if has else '查询结果：订阅方未持有该seq，现在补投'),
            duplicate=False, applied=False)
        if has:
            apply_ack(name, seq, now_ms, lost=False, out_of_order=False,
                      reconciled=True)
        else:
            sub['inflight'].pop(seq, None)
            schedule_delivery(sub, seq, now_ms, '查清后补投', extra['attempt'] + 1)

    def apply_ack(name, seq, at_ms, lost, out_of_order, reconciled=False):
        sub = subscribers[name]
        if seq not in changes:
            raise ValueError('回执引用了不存在的变更序号：' + str(seq))
        if lost:
            if seq <= sub['delivered']:
                sub['received'].add(seq)
            log('ack', at_ms, subscriber=name, seq=seq, lost=True,
                out_of_order=out_of_order, advanced=False,
                reason='回执在网络中丢失，订阅方已入账，代理进度不推进，等待超时机制')
            return
        expected = sub['acked'] + 1
        if seq < expected:
            log('ack', at_ms, subscriber=name, seq=seq, lost=False,
                out_of_order=False, advanced=False,
                reason='重复回执，该变更此前已确认，忽略')
            return
        if seq > expected:
            sub['known_acks'].add(seq)
            log('ack', at_ms, subscriber=name, seq=seq, lost=False,
                out_of_order=True, advanced=False,
                reason='乱序回执：seq %d 先到，未确认的 seq %d 挡在前面，进度不越过'
                       % (seq, expected))
            return
        for k in ('ack_timeout', 'probe_send', 'probe_reply', 'deliver', 'vack'):
            cancel(k, name, seq)
        sub['inflight'].pop(seq, None)
        sub['acked'] = seq
        while sub['acked'] + 1 in sub['known_acks']:
            nxt = sub['acked'] + 1
            sub['known_acks'].discard(nxt)
            for k in ('ack_timeout', 'probe_send', 'probe_reply', 'deliver', 'vack'):
                cancel(k, name, nxt)
            sub['inflight'].pop(nxt, None)
            sub['acked'] = nxt
            log('ack', at_ms, subscriber=name, seq=nxt, lost=False,
                out_of_order=False, advanced=True,
                reason='此前乱序到达的回执补位确认，连续推进订阅进度')
        log('ack', at_ms, subscriber=name, seq=seq, lost=False,
            out_of_order=out_of_order, advanced=True,
            reason=('查询确认后补登回执，进度推进' if reconciled
                    else '按序回执确认，订阅进度推进'))
        advance(sub, at_ms)

    def handle_input(event, now):
        nonlocal next_seq
        kind = event['kind']
        if kind in ('node_down', 'node_up'):
            target = nodes.get(event.get('node'))
            if not target:
                raise ValueError('事件引用未知节点：' + str(event.get('node')))
            target['status'] = 'down' if kind == 'node_down' else 'healthy'
            log(kind, now, node=target['name'],
                reason=('主库下线，供数责任转移给达标副本'
                        if kind == 'node_down' and target['role'] == 'primary'
                        else ('节点恢复' if kind == 'node_up' else '节点下线')))
            if kind == 'node_up':
                for sub in subscribers.values():
                    if sub['status'] == 'healthy':
                        advance(sub, now)
        elif kind == 'replicate':
            target = nodes.get(event.get('node'))
            if not target:
                raise ValueError('事件引用未知节点：' + str(event.get('node')))
            if target['role'] != 'replica':
                raise ValueError('replicate仅允许副本节点')
            if target['status'] != 'healthy':
                raise ValueError('停机副本不能应用日志，请先node_up')
            if target['lineage'] != lineage:
                raise ValueError('日志谱系不匹配，不得复制当前主库日志')
            pos = event['position']
            if not target['position'] <= pos <= primary['position']:
                raise ValueError('复制位点必须单调且不超过主库已提交位点')
            target['position'] = pos
            target['delay_ms'] = event['delay_ms']
            log('replicate', now, node=target['name'], position=pos,
                delay_ms=event['delay_ms'], reason='副本应用已提交日志')
            for sub in subscribers.values():
                if sub['status'] == 'healthy':
                    advance(sub, now)
        elif kind == 'write':
            key = event.get('key', 'key-%d' % next_seq)
            if primary['status'] != 'healthy':
                log('write', now, key=key, value=event.get('value'),
                    committed=False, seq=None,
                    reason='主库不可用，写入失败，不生成变更序号')
                return
            primary['position'] += 1
            seq = next_seq
            next_seq += 1
            changes[seq] = {'at_ms': now, 'key': key,
                            'value': event.get('value'),
                            'position': primary['position']}
            log('write', now, key=key, value=event.get('value'),
                committed=True, seq=seq, position=primary['position'],
                reason='写入主库成功，生成有序变更，可开始供数')
            for sub in subscribers.values():
                if sub['status'] == 'healthy':
                    advance(sub, now)
        elif kind == 'subscribe':
            name = event['subscriber']
            if name in subscribers:
                raise ValueError('订阅方已存在，不得重复订阅：' + name)
            start = event.get('from_position_seq', 0)
            if start < 0 or start >= next_seq:
                raise ValueError('订阅起点必须位于已提交变更区间内')
            subscribers[name] = {
                'name': name, 'status': 'healthy',
                'acked': start, 'delivered': start, 'applied': start,
                'received': set(range(1, start + 1)),
                'known_acks': set(), 'inflight': {}, 'blocked': None,
                'duplicates': 0, 'subscribed_at': now,
            }
            log('subscribe', now, subscriber=name, from_seq=start + 1,
                reason='建立订阅，从下一个未确认变更开始按序投递')
            advance(subscribers[name], now)
        elif kind in ('sub_down', 'sub_up'):
            name = event['subscriber']
            if name not in subscribers:
                raise ValueError('事件引用未知订阅方：' + name)
            sub = subscribers[name]
            sub['status'] = 'down' if kind == 'sub_down' else 'healthy'
            log(kind, now, subscriber=name,
                reason=('订阅连接中断，在途投递与回执暂停'
                        if kind == 'sub_down'
                        else '订阅恢复，从未确认位点重试，已入账变更幂等跳过'))
            if kind == 'sub_up':
                # Re-arm the earliest unconfirmed change; old timers are stale.
                for s in list(sub['inflight']):
                    sub['inflight'].pop(s, None)
                advance(sub, now)
        elif kind == 'ack':
            name = event['subscriber']
            if name not in subscribers:
                raise ValueError('事件引用未知订阅方：' + name)
            apply_ack(name, event['seq'], now,
                      lost=bool(event.get('lost', False)),
                      out_of_order=bool(event.get('out_of_order', False)))

    # ---- unified discrete event loop: merge input events and virtual timers ----
    input_queue = list(events)
    input_order = 0
    guard = 0
    while input_queue or timers:
        guard += 1
        if guard > 20000:
            raise ValueError('模拟未能收敛，请检查回执与故障事件是否闭环')
        if timers and (not input_queue or timers[0]['at'] <= input_queue[0]['at_ms']):
            t = timers.pop(0)
            now = t['at']
            if t['kind'] == 'deliver':
                do_deliver(t, now)
            elif t['kind'] == 'ack_timeout':
                do_timeout(t, now)
            elif t['kind'] == 'probe_reply':
                do_probe_reply(t, now)
            elif t['kind'] == 'vack':
                # Idempotent re-ack only confirms the next contiguous change;
                # a gap means an earlier change is still unconfirmed, so hold.
                if t['seq'] == subscribers[t['sub']]['acked'] + 1:
                    apply_ack(t['sub'], t['seq'], now, lost=False, out_of_order=False)
        else:
            event = input_queue.pop(0)
            input_order += 1
            handle_input(event, event['at_ms'])

    timeline.sort(key=lambda r: (r['at_ms'], r.get('index', 0)))
    for i, row in enumerate(timeline, start=1):
        row['index'] = i

    total = next_seq - 1
    sub_results = []
    for name, sub in sorted(subscribers.items()):
        caught = sub['acked'] >= total
        catchup_ms = next((r['at_ms'] - sub['subscribed_at']
                           for r in timeline
                           if r['kind'] == 'ack' and r.get('subscriber') == name
                           and r.get('seq') == total and r.get('advanced')), None)
        sub_results.append({
            'name': name, 'status': sub['status'],
            'acked_seq': sub['acked'], 'delivered_seq': sub['delivered'],
            'applied_seq': sub['applied'], 'total_committed': total,
            'caught_up': caught, 'catchup_ms': catchup_ms,
            'duplicate_deliveries': sub['duplicates'],
            'out_of_order_acks_held': sorted(sub['known_acks']),
            'blocked_reason': sub['blocked']['reason'] if sub['blocked'] else '',
        })

    result = {
        'mode': '受控订阅投递模型 / 非真实消息中间件',
        'lineage': lineage,
        'policy': {
            'ack_policy': policy,
            'delivery_ms': delivery_ms,
            'ack_timeout_ms': ack_timeout_ms,
            'reconcile_rtt_ms': reconcile_rtt_ms,
            'inflight_window': inflight_window,
            'max_retries': MAX_RETRIES,
            'resend_tradeoff': 'resend：超时即重发，追平快但可能重复收件，靠seq幂等去重',
            'reconcile_tradeoff': 'reconcile：先查订阅方已收位点再决定补投，零重复收件但多一次查询往返',
            'ordering_rule': '订阅进度只按连续回执推进，乱序或丢失回执不能越过未确认变更',
        },
        'committed_changes': [
            {'seq': seq, **{k: changes[seq][k]
                            for k in ('at_ms', 'key', 'value', 'position')}}
            for seq in sorted(changes)
        ],
        'subscribers': sub_results,
        'final_nodes': list(nodes.values()),
        'timeline': timeline,
        'summary': {
            'committed': total,
            'failed_writes': sum(1 for r in timeline
                                 if r['kind'] == 'write' and not r['committed']),
            'deliveries': sum(1 for r in timeline
                              if r['kind'] == 'deliver' and r['arrived']),
            'duplicate_deliveries': sum(s['duplicate_deliveries'] for s in sub_results),
            'lost_acks': sum(1 for r in timeline
                             if r['kind'] == 'ack' and r.get('lost')),
            'reconcile_probes': sum(1 for r in timeline
                                    if r['kind'] == 'probe_send'),
            'all_caught_up': all(s['caught_up'] for s in sub_results)
                              if sub_results else False,
            # Idempotency invariant: each seq is actually applied at most once,
            # no matter how many duplicate retries arrive.
            'no_double_apply': all(
                len([r for r in timeline
                     if r['kind'] == 'deliver' and r.get('applied')
                     and r.get('subscriber') == s['name'] and r.get('seq') == seq]) <= 1
                for s in sub_results for seq in changes),
        },
    }
    result['checksum'] = hashlib.sha256(
        json.dumps(result, sort_keys=True, ensure_ascii=False).encode()
    ).hexdigest()
    return result
