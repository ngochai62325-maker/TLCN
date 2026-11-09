"""Independent Python reference calculations for model/filter acceptance."""
from decimal import Decimal


def paired_ratio(rows,numerator,denominator,factor=1):
    pairs=[(r[numerator],r[denominator]) for r in rows if r[numerator] is not None and r[denominator] is not None and r[denominator]>0]
    return (sum((n for n,d in pairs),Decimal(0))*factor/sum((d for n,d in pairs),Decimal(0))) if pairs else None


def period_level(rows,period,value,identity=()):
    """A stock/price/annual level is undefined across periods or mixed identity."""
    if not rows or len({r[period] for r in rows})!=1:return None
    if identity and len({tuple(r[c] for c in identity) for r in rows})!=1:return None
    values=[r[value] for r in rows if r[value] is not None]
    return sum(values,Decimal(0)) if values else None


def endpoints(rows,period,value):
    cells={r[period]:r[value] for r in rows if r[value] is not None}
    if not cells:return None
    lo,hi=min(cells),max(cells);peak=max(cells.values());peak_year=min(y for y,v in cells.items() if v==peak)
    return {'first_period':lo,'last_period':hi,'first':cells[lo],'last':cells[hi],
        'change_pct':(cells[hi]/cells[lo]-1)*100 if hi>lo and cells[lo]>0 else None,
        'peak':peak,'peak_period':peak_year,'peak_to_last_pct':(cells[hi]/peak-1)*100 if peak>0 else None}


def yoy(rows,year,value,selected_year):
    cells={r[year]:r[value] for r in rows}
    a,b=cells.get(selected_year-1),cells.get(selected_year)
    return (b/a-1)*100 if a is not None and a>0 and b is not None else None
