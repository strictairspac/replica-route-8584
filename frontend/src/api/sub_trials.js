import {api} from './client.js';
import {session} from '../hooks/session.js';
export const subTrialsApi={list:()=>api('/sub_trials'),create:data=>api('/sub_trials','POST',data)};
export async function downloadSub(id){const res=await fetch('/api/sub_trials/'+id+'/export',{headers:{Authorization:'Bearer '+session.token}});if(!res.ok)throw Error('导出失败，请重新登录后重试');const url=URL.createObjectURL(await res.blob());const a=document.createElement('a');a.href=url;a.download='sub-'+id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);}
