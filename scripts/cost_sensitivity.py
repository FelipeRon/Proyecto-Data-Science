"""Illustrative decision simulation, not observed maintenance savings."""
import csv,json
from pathlib import Path
root=Path(__file__).resolve().parents[1]
m=json.loads((root/'docs/baseline_results.json').read_text())
rows=[]
for name,cp,ce,cr in [('central',500,8000,1500),('low_emergency',500,3000,1500),('high_emergency',500,15000,1500),('high_inspection',1000,8000,1500),('low_inspection',250,8000,1500),('no_early_benefit',500,8000,8000)]:
 cost=(m['tp']+m['fp'])*cp + m['fn']*ce + m['tp']*cr
 no=(m['tp']+m['fn'])*ce
 rows.append(dict(scenario=name,inspection=cp,emergency=ce,residual=cr,threshold=cp/(ce-cr) if ce>cr else None,baseline_cost=cost,no_alert_cost=no,savings_pct=round(100*(no-cost)/no,2)))
with (root/'docs/cost_sensitivity.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=rows[0],lineterminator='\n');w.writeheader();w.writerows(rows)
print(json.dumps(rows,indent=2))

# Operating sensitivity: cap inspections, rank only rule-positive observations,
# by severity=max(wear/200, torque/60), ties by UDI; never rank by target.
import duckdb, math
con=duckdb.connect(str(root/'artifacts/local.duckdb'),read_only=True)
data=con.execute('select reading_id, machine_failure, baseline_alert, greatest(tool_wear_min/200.0,torque_nm/60.0) severity from gold.fct_reading order by severity desc, reading_id').fetchall()
capacities=[]
for fraction in [.05,.08,.12,.20]:
    remaining=math.floor(fraction*len(data)); tp=fp=fn=tn=0
    for _,target,candidate,_ in data:
        alert=bool(candidate and remaining>0)
        if alert: remaining-=1
        if target and alert: tp+=1
        elif alert: fp+=1
        elif target: fn+=1
        else: tn+=1
    cost=(tp+fp)*500+fn*8000+tp*1500
    capacities.append(dict(capacity_pct=fraction*100,alerts=tp+fp,tp=tp,fp=fp,fn=fn,tn=tn,recall=round(tp/(tp+fn),4),cost=cost,savings_pct=round(100*(2712000-cost)/2712000,2)))
with (root/'docs/capacity_sensitivity.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=capacities[0],lineterminator='\n');w.writeheader();w.writerows(capacities)
print(json.dumps(capacities,indent=2))
