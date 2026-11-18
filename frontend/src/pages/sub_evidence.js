import {badge} from '../components/common/badge.js';
import {esc,date} from '../utils/html.js';
import {downloadSub} from '../api/sub_trials.js';
import {feedback} from '../hooks/feedback.js';

const KIND_LABEL={write:'写入',replicate:'复制',node_down:'节点下线',node_up:'节点恢复',subscribe:'建立订阅',sub_down:'订阅中断',sub_up:'订阅恢复',ack:'回执',deliver:'投递',ack_timeout:'回执超时',probe_send:'发起查清',probe_reply:'查清结果'};

function lane(kind){
  if(kind==='write')return 'write';
  if(kind==='replicate'||kind==='node_down'||kind==='node_up')return 'replica';
  if(kind==='deliver'||kind==='ack_timeout'||kind==='probe_send'||kind==='probe_reply')return 'delivery';
  if(kind==='ack')return 'ack';
  return 'lifecycle';
}

function seqCell(row){
  if(row.kind==='write')return row.committed?`#${row.seq}`:'<span class="rejected">失败</span>';
  if(row.seq!=null)return `#${row.seq}`;
  return '—';
}

function targetCell(row){
  if(row.node)return esc(row.node);
  if(row.source)return esc(row.source);
  if(row.subscriber)return esc(row.subscriber);
  return '—';
}

export function subEvidencePage(trial){
  if(!trial)return '<div class="empty">还没有订阅投递证据。先编排订阅场景，再从「订阅演练」运行一次。</div>';
  const r=trial.result;
  const bindDownload=()=>{document.querySelector('#sub-export')?.addEventListener('click',async()=>{try{await downloadSub(trial.id);feedback('订阅投递JSON已下载')}catch(e){feedback(e.message,true);}});};
  setTimeout(bindDownload,0);
  if(!r)return `<div class="evidence-top"><div><span class="eyebrow">SUBSCRIPTION EVIDENCE / #${trial.id}</span><h2>${esc(trial.name)} ${badge(trial.status)}</h2></div></div><div class="notice">后台重放中，每 1.5 秒刷新。可保留此页，或稍后从演练记录返回。</div>`;
  const maxAt=Math.max(1,...r.timeline.map(x=>x.at_ms));
  const lanes=['write','replica','lifecycle','delivery','ack'];
  const laneName={write:'写入航道',replica:'复制 / 节点',lifecycle:'订阅生命周期',delivery:'投递 / 重试',ack:'回执'};
  return `<div class="evidence-top"><div><span class="eyebrow">SUBSCRIPTION EVIDENCE / #${trial.id}</span><h2>${esc(trial.name)} ${badge(trial.status)}</h2><p>${date(trial.created_at)} · 节点与场景快照已冻结</p></div><button id="sub-export">导出 JSON ↓</button></div>
${trial.error?`<div class="error">${esc(trial.error)}。请检查场景并重新执行。</div>`:''}
<div class="verdict"><div class="verdict-icon">${r.summary.all_caught_up?'✓':'!'}</div><div><span class="eyebrow">SUBSCRIPTION CAUGHT-UP CHECK</span><h2>${r.summary.all_caught_up?'全部订阅方已追平已提交变更':'仍有订阅方停在未确认位点'}</h2><p>已提交 ${r.summary.committed} · 失败写入 ${r.summary.failed_writes} · 成功投递 ${r.summary.deliveries} · 重复收件 ${r.summary.duplicate_deliveries} · 丢失回执 ${r.summary.lost_acks}</p></div></div>
<div class="policy-strip">策略 ${r.policy.ack_policy==='resend'?'超时重发 resend':'查清再补 reconcile'}<span>／</span>投递 ${r.policy.delivery_ms}ms<span>／</span>回执超时 ${r.policy.ack_timeout_ms}ms<span>／</span>在途窗口 ${r.policy.inflight_window}<span>／</span>谱系 ${esc(r.lineage)}</div>
<div class="sub-grid">
<section><div class="section-top"><h2>订阅方进度</h2><span>ACKED / DELIVERED / TOTAL</span></div>${r.subscribers.map(s=>`<div class="key-proof"><strong>${esc(s.name)} ${s.caught_up?'<span class="badge completed">已追平</span>':'<span class="badge running">积压中</span>'}</strong><p>已确认位点 <b>${s.acked_seq}</b> ／ 已投递 ${s.delivered_seq} ／ 已提交 ${s.total_committed}　追平耗时 ${s.catchup_ms==null?'未追平':s.catchup_ms+'ms'}　重复收件 ${s.duplicate_deliveries}</p>${s.blocked_reason?`<p class="rejected">阻塞：${esc(s.blocked_reason)}</p>`:''}${s.out_of_order_acks_held.length?`<p>暂存的乱序回执：${s.out_of_order_acks_held.map(x=>'#'+x).join('、')}</p>`:''}</div>`).join('')}</section>
<section><div class="section-top"><h2>已提交变更（有序 seq）</h2><span>WRITE-THEN-SERVE</span></div>${r.committed_changes.map(c=>`<div class="key-proof"><strong>#${c.seq} ${esc(c.key||'')}</strong><p>值 ${esc(c.value??'—')} ／ 提交于 ${c.at_ms}ms ／ 主库位点 ${c.position}</p></div>`).join('')}</section>
</div>
<div class="section-top"><h2>写入 / 复制 / 故障 / 回执 时序泳道</h2><span>${r.timeline.length} EVENTS · 从积压到追平</span></div>
<div class="swim-wrap"><div class="swim-axis"><span></span>${[0,.25,.5,.75,1].map(f=>`<i style="left:${f*100}%">${Math.round(f*maxAt)}ms</i>`).join('')}</div>${lanes.map(l=>`<div class="swim-lane"><div class="swim-label">${laneName[l]}</div><div class="swim-track">${r.timeline.filter(x=>lane(x.kind)===l).map(x=>{const left=(x.at_ms/maxAt)*100;const tone=x.duplicate?'dup':(x.lost?'lost':(x.arrived===false?'miss':(x.advanced?'ok':(x.out_of_order?'hold':'norm'))));return `<div class="swim-dot ${tone}" style="left:${left}%" title="${x.at_ms}ms ${KIND_LABEL[x.kind]||x.kind} ${x.seq!=null?'#'+x.seq:''}"><span class="swim-time">${x.at_ms}</span><b>${KIND_LABEL[x.kind]||x.kind}${x.seq!=null?' #'+x.seq:''}</b></div>`;}).join('')}</div></div>`).join('')}</div>
<div class="table-wrap"><table><thead><tr><th>时间</th><th>类型</th><th>变更</th><th>节点 / 订阅方</th><th>结果与理由</th></tr></thead><tbody>${r.timeline.map(x=>`<tr><td>${x.at_ms}ms</td><td><span class="badge ${lane(x.kind)}">${KIND_LABEL[x.kind]||x.kind}</span></td><td>${seqCell(x)}</td><td>${targetCell(x)}</td><td>${x.duplicate?'<span class="rejected">重复收件·幂等去重</span>':(x.lost?'<span class="rejected">回执丢失</span>':(x.arrived===false?'<span class="rejected">未到达</span>':(x.out_of_order?'<span class="hold-tag">乱序暂存</span>':(x.advanced?'<span class="ok-tag">进度推进</span>':''))))} ${esc(x.reason||'')}</td></tr>`).join('')}</tbody></table></div>
<div class="evidence-notes"><section><h3>追平与去重口径</h3><p>${esc(r.policy.ordering_rule)}。</p><p>${esc(r.policy.ack_policy==='resend'?r.policy.resend_tradeoff:r.policy.reconcile_tradeoff)}</p><p>自动重发上限 ${r.policy.max_retries} 次，超过后保留未确认状态，避免演练不收敛。</p></section><section><h3>确定性摘要</h3><p>${esc(r.mode)}；命名谱系单调整数位点，非真实消息中间件。</p><code>${esc(r.checksum)}</code></section></div>`;
}
