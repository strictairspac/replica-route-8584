import {table} from '../components/common/table.js';
import {esc} from '../utils/html.js';
import {session} from '../hooks/session.js';
const sample=[
  {at_ms:0,kind:'write',key:'signal:1',value:'OPEN'},
  {at_ms:30,kind:'subscribe',subscriber:'watchtower'},
  {at_ms:500,kind:'ack',subscriber:'watchtower',seq:1,lost:true},
];
const helpRows=[
  ['write','写入主库，成功才生成有序变更；主库下线则失败，不占序号。可带 key/value'],
  ['replicate','副本应用日志，需要 node/position/delay_ms，位点单调且不超主库'],
  ['node_down / node_up','主库或副本下线/恢复，需要 node；主库下线后达标副本接力'],
  ['subscribe','建立订阅，需要 subscriber，可选 from_position_seq 指定起点'],
  ['sub_down / sub_up','订阅连接中断/恢复，需要 subscriber；恢复后从未确认位点重试'],
  ['ack','回执，需要 subscriber/seq；lost:true 丢回执，out_of_order:true 乱序回执'],
];
export function subScenariosPage(items){return `<div class="split"><section><div class="section-top"><h2>订阅投递时序场景</h2><span>${items.length} SCENARIOS / DELIVERY REPLAY</span></div>${table(['场景 / 说明','事件数','操作'],items.map(r=>[`<strong>${esc(r.code)}</strong><small>${esc(r.title)}</small><small>${esc(r.note)}</small>`,`${r.events.length} 个`,`<button class="small edit-sub-scenario" data-id="${r.id}" ${session.user.role==='viewer'?'disabled':''}>编辑</button>`]))}<div class="protocol"><h3>订阅事件字段速查</h3>${table(['事件','含义与字段'],helpRows.map(([k,v])=>[`<strong>${esc(k)}</strong>`,esc(v)]))}<p>事件按 at_ms 非递减排列，同一时刻按输入先后，最多 100 条。投递、超时、探测等虚拟事件由引擎按策略自动生成，不需要手工编排。</p></div></section><section class="form-panel"><span class="eyebrow">SUBSCRIPTION SCENARIO</span><h2 id="sub-scenario-heading">编排订阅场景</h2><form id="sub-scenario-form"><input type="hidden" name="id"><label>场景编号<input name="code" placeholder="SUB-E" required></label><label>工作场景<input name="title" value="主库故障下的订阅接力" required></label><label>时序事件 JSON<textarea name="events" rows="13" required>${esc(JSON.stringify(sample,null,2))}</textarea></label><label>备注<input name="note" value="写入 / 复制 / 故障 / 回执时序"></label><button class="primary" ${session.user.role==='viewer'?'disabled':''}>保存订阅场景 +</button></form></section></div>`;}
