"""Presentation/provenance helpers. Metric formulas are in notebook cells."""
import json
from pathlib import Path


def inspect_frame(frame):
    import pandas as pd
    return pd.DataFrame({"dtype":frame.dtypes.astype(str),"nulls":frame.isna().sum()})


def label(text):
    items=json.loads(text)
    return " | ".join(dict.fromkeys(str(x.get('label')) if isinstance(x,dict) else str(x) for x in items))


def resolve(frame, namespace, identity, key_name):
    matched=frame[(frame.identity_namespace==namespace)&(frame.source_identity==identity)]
    if len(matched)!=1:
        raise ValueError(f"Missing/ambiguous source identity: {namespace}/{identity}")
    return matched.iloc[0][key_name]


def annual_plot(frame,value,title,unit,year="year_key",group=None):
    import pandas as pd
    if frame.empty or not pd.to_numeric(frame[value],errors='raise').notna().any():
        raise ValueError("No numeric observations in this scope; refuse a blank analytical chart")
    import matplotlib.pyplot as plt
    fig,ax=plt.subplots(figsize=(9,4))
    groups=frame.groupby(group,dropna=False) if group else [(None,frame)]
    for name,data in groups:
        data=data.sort_values(year)
        if data.duplicated(year).any():raise ValueError("Plot grain collision")
        x=data.set_index(year)[value]
        x=pd.to_numeric(x,errors='raise').reindex(range(int(x.index.min()),int(x.index.max())+1))
        ax.plot(x.index,x.values,label=str(name) if name is not None else None)
    ax.set(title=title,xlabel="Year / source reporting label",ylabel=unit)
    ax.grid(alpha=.2)
    if group:ax.legend(fontsize=8)
    fig.text(.01,.005,"Source: published TLCN Gold; scope and snapshot in notebook query evidence",fontsize=8)
    fig.tight_layout(rect=(0,.04,1,1));plt.show();plt.close(fig)


def save_evidence(root,slug,client,findings):
    folder=Path(root)/'docs/analytics/evidence'
    folder.mkdir(exist_ok=True)
    path=folder/(slug+'.json')
    payload={"notebook":slug,"batch":client.batch,"queries":client.evidence,"findings":findings}
    path.write_text(json.dumps(payload,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
    return path.relative_to(root).as_posix()
