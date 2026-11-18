import {subScenariosApi} from '../api/sub_scenarios.js';
export const subScenariosStore={items:[],async refresh(){this.items=await subScenariosApi.list();return this.items;}};
