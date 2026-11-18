import {subTrialsApi} from '../api/sub_trials.js';
export const subTrialsStore={items:[],async refresh(){this.items=await subTrialsApi.list();return this.items;}};
