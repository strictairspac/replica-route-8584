from sqlalchemy import select,func
from src.config.database import Session,Base,engine
from src.models.entities import User,Node,RequestSet,Trial,SubScenario,SubTrial
from src.utils.security import password_hash

def seed():
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin','operator','viewer'):
            email=role+'@qiyuan.test'
            if not db.scalar(select(User).where(User.email==email)):db.add(User(email=email,password=password_hash('Demo2026!'),role=role))
        for name,role,pos,delay,note in [('harbor-primary','primary',105,0,'栖原主站 · 写入航道'),('north-replica','replica',104,150,'北站副本 · 延迟偏高'),('south-replica','replica',105,20,'南站副本 · 常用读航道')]:
            if not db.scalar(select(Node).where(Node.name==name)):db.add(Node(name=name,role=role,status='healthy',lineage='qiyuan-log-01',position=pos,delay_ms=delay,note=note))
        mixed=[{'at_ms':0,'kind':'write','session':'lab-A'},{'at_ms':100,'kind':'read','session':'lab-A'},{'at_ms':600,'kind':'replicate','node':'south-replica','position':106,'delay_ms':20},{'at_ms':700,'kind':'read','session':'lab-A'},{'at_ms':800,'kind':'node_down','node':'south-replica'},{'at_ms':900,'kind':'read','session':'lab-A'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-A'}]
        cases=[('QY-1117-A','交接班写后读与双节点故障',mixed),('QY-1117-B','延迟超标回退主库',[{'at_ms':0,'kind':'node_down','node':'south-replica'},{'at_ms':10,'kind':'read','session':'visitor'}]),('QY-1117-C','双副本不可读时拒绝',[{'at_ms':0,'kind':'write','session':'lab-B'},{'at_ms':1000,'kind':'node_down','node':'harbor-primary'},{'at_ms':1100,'kind':'read','session':'lab-B'}])]
        for code,title,events in cases:
            if not db.scalar(select(RequestSet).where(RequestSet.code==code)):db.add(RequestSet(code=code,title=title,events=events,note='栖原站点值班交接 · 09:15 请求轨迹'))
        sub_cases=[
            ('SUB-A','主库故障后副本接力供数与缺位点等待',[
                {'at_ms':0,'kind':'write','key':'signal:1','value':'OPEN'},
                {'at_ms':20,'kind':'subscribe','subscriber':'watchtower'},
                {'at_ms':40,'kind':'write','key':'signal:2','value':'ARM'},
                {'at_ms':80,'kind':'node_down','node':'harbor-primary'},
                {'at_ms':120,'kind':'write','key':'signal:3','value':'SHOULD-FAIL'},
                {'at_ms':300,'kind':'replicate','node':'south-replica','position':107,'delay_ms':20},
            ]),
            ('SUB-B','丢回执：超时重发的重复收件与幂等去重',[
                {'at_ms':0,'kind':'write','key':'signal:1','value':'OPEN'},
                {'at_ms':30,'kind':'subscribe','subscriber':'watchtower'},
                {'at_ms':500,'kind':'ack','subscriber':'watchtower','seq':1,'lost':True},
                {'at_ms':900,'kind':'write','key':'signal:2','value':'ARM'},
            ]),
            ('SUB-C','订阅中断恢复：未确认位点重试不重复入账',[
                {'at_ms':0,'kind':'write','key':'signal:1','value':'OPEN'},
                {'at_ms':30,'kind':'subscribe','subscriber':'watchtower'},
                {'at_ms':120,'kind':'sub_down','subscriber':'watchtower'},
                {'at_ms':700,'kind':'sub_up','subscriber':'watchtower'},
                {'at_ms':900,'kind':'write','key':'signal:2','value':'ARM'},
            ]),
            ('SUB-D','乱序回执：进度不越过未确认变更',[
                {'at_ms':0,'kind':'write','key':'signal:1','value':'OPEN'},
                {'at_ms':20,'kind':'write','key':'signal:2','value':'ARM'},
                {'at_ms':40,'kind':'write','key':'signal:3','value':'HOLD'},
                {'at_ms':60,'kind':'subscribe','subscriber':'watchtower'},
                {'at_ms':220,'kind':'ack','subscriber':'watchtower','seq':2,'out_of_order':True},
                {'at_ms':260,'kind':'ack','subscriber':'watchtower','seq':1},
                {'at_ms':300,'kind':'ack','subscriber':'watchtower','seq':3},
            ]),
        ]
        for code,title,events in sub_cases:
            if not db.scalar(select(SubScenario).where(SubScenario.code==code)):db.add(SubScenario(code=code,title=title,events=events,note='订阅投递演练 · 写入/复制/故障/回执时序'))
        db.commit();print({cls.__tablename__:db.scalar(select(func.count()).select_from(cls)) for cls in (User,Node,RequestSet,Trial,SubScenario,SubTrial)})
if __name__=='__main__':seed()
