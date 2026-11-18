import {api} from './client.js';
export const subScenariosApi={list:()=>api('/sub_scenarios'),create:data=>api('/sub_scenarios','POST',data),update:(id,data)=>api('/sub_scenarios/'+id,'PUT',data)};
