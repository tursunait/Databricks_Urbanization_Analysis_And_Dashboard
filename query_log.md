```sql
SELECT COUNT(*) AS count 
FROM default.urbanization_statedb_tt284 us RIGHT JOIN default.urbanizationdb_tt284 u on us.state=u.state;
```

```response from databricks
[Row(count=260)]
```

```sql
SELECT us.state, round(avg(u.urbanindex)) AS urbanindex
FROM default.urbanization_statedb_tt284 us RIGHT JOIN default.urbanizationdb_tt284 u on us.state=u.state
group by us.state
order by urbanindex desc;
```

```response from databricks
[Row(state='New York', urbanindex=15.0), Row(state='New Jersey', urbanindex=15.0), Row(state='Maryland', urbanindex=14.0), Row(state='Pennsylvania', urbanindex=14.0), Row(state='California', urbanindex=14.0), Row(state='Illinois', urbanindex=14.0), Row(state='Massachusetts', urbanindex=14.0), Row(state='Virginia', urbanindex=14.0), Row(state='District of Columbia', urbanindex=14.0), Row(state='Ohio', urbanindex=13.0), Row(state='Nebraska', urbanindex=13.0), Row(state='Washington', urbanindex=13.0), Row(state='Indiana', urbanindex=13.0), Row(state='Louisiana', urbanindex=13.0), Row(state='New Mexico', urbanindex=13.0), Row(state='Texas', urbanindex=13.0), Row(state='Colorado', urbanindex=13.0), Row(state='Oregon', urbanindex=13.0), Row(state='Nevada', urbanindex=13.0), Row(state='Hawaii', urbanindex=13.0), Row(state='Arizona', urbanindex=13.0), Row(state='Florida', urbanindex=13.0), Row(state='Michigan', urbanindex=13.0), Row(state='North Carolina', urbanindex=13.0), Row(state='Rhode Island', urbanindex=13.0), Row(state='Kentucky', urbanindex=13.0), Row(state='Connecticut', urbanindex=13.0), Row(state='Georgia', urbanindex=13.0), Row(state='Minnesota', urbanindex=13.0), Row(state='Utah', urbanindex=13.0), Row(state='Puerto Rico', urbanindex=13.0), Row(state='Missouri', urbanindex=13.0), Row(state='Kansas', urbanindex=13.0), Row(state='Wisconsin', urbanindex=13.0), Row(state='Mississippi', urbanindex=12.0), Row(state='Maine', urbanindex=12.0), Row(state='South Dakota', urbanindex=12.0), Row(state='Oklahoma', urbanindex=12.0), Row(state='Alabama', urbanindex=12.0), Row(state='North Dakota', urbanindex=12.0), Row(state='Delaware', urbanindex=12.0), Row(state='Tennessee', urbanindex=12.0), Row(state='South Carolina', urbanindex=12.0), Row(state='Alaska', urbanindex=12.0), Row(state='Arkansas', urbanindex=12.0), Row(state='Idaho', urbanindex=12.0), Row(state='Montana', urbanindex=12.0), Row(state='Iowa', urbanindex=12.0), Row(state='New Hampshire', urbanindex=12.0), Row(state='West Virginia', urbanindex=11.0), Row(state='Vermont', urbanindex=11.0), Row(state='Wyoming', urbanindex=11.0)]
```

```sql
SELECT u.state, round(avg(us.urbanindex)) AS urbanindex
FROM default.urbanization_statedb_tt284 us RIGHT JOIN default.urbanizationdb_tt284 u on us.state=u.state
group by u.state
order by urbanindex desc;
```

```response from databricks
[Row(state='New York', urbanindex=13.0), Row(state='District of Columbia', urbanindex=13.0), Row(state='Maryland', urbanindex=12.0), Row(state='California', urbanindex=12.0), Row(state='Illinois', urbanindex=12.0), Row(state='Massachusetts', urbanindex=12.0), Row(state='Nevada', urbanindex=12.0), Row(state='Rhode Island', urbanindex=12.0), Row(state='Puerto Rico', urbanindex=12.0), Row(state='New Jersey', urbanindex=12.0), Row(state='Ohio', urbanindex=11.0), Row(state='Pennsylvania', urbanindex=11.0), Row(state='Washington', urbanindex=11.0), Row(state='Texas', urbanindex=11.0), Row(state='Colorado', urbanindex=11.0), Row(state='Delaware', urbanindex=11.0), Row(state='Oregon', urbanindex=11.0), Row(state='Hawaii', urbanindex=11.0), Row(state='Arizona', urbanindex=11.0), Row(state='Florida', urbanindex=11.0), Row(state='Michigan', urbanindex=11.0), Row(state='Connecticut', urbanindex=11.0), Row(state='Georgia', urbanindex=11.0), Row(state='Utah', urbanindex=11.0), Row(state='Virginia', urbanindex=11.0), Row(state='Wisconsin', urbanindex=10.0), Row(state='Nebraska', urbanindex=10.0), Row(state='Oklahoma', urbanindex=10.0), Row(state='Alabama', urbanindex=10.0), Row(state='Indiana', urbanindex=10.0), Row(state='Louisiana', urbanindex=10.0), Row(state='New Mexico', urbanindex=10.0), Row(state='Tennessee', urbanindex=10.0), Row(state='North Carolina', urbanindex=10.0), Row(state='South Carolina', urbanindex=10.0), Row(state='Kentucky', urbanindex=10.0), Row(state='Idaho', urbanindex=10.0), Row(state='Minnesota', urbanindex=10.0), Row(state='Missouri', urbanindex=10.0), Row(state='Iowa', urbanindex=10.0), Row(state='Kansas', urbanindex=10.0), Row(state='New Hampshire', urbanindex=10.0), Row(state='Mississippi', urbanindex=9.0), Row(state='West Virginia', urbanindex=9.0), Row(state='Maine', urbanindex=9.0), Row(state='South Dakota', urbanindex=9.0), Row(state='North Dakota', urbanindex=9.0), Row(state='Vermont', urbanindex=9.0), Row(state='Alaska', urbanindex=9.0), Row(state='Arkansas', urbanindex=9.0), Row(state='Wyoming', urbanindex=8.0), Row(state='Montana', urbanindex=8.0)]
```

```sql
SELECT round(avg(us.urbanindex)) AS urbanindex_state, round(avg(u.urbanindex)) AS urbanindex
FROM default.urbanization_statedb_tt284 us RIGHT JOIN default.urbanizationdb_tt284 u on us.state=u.state;
```

```response from databricks
[Row(urbanindex_state=10.0, urbanindex=13.0)]
```

```sql
SELECT u.state, 
       (MAX(u.urbanindex) - MIN(u.urbanindex)) AS urbanization_change
FROM default.urbanizationdb_tt284 u
GROUP BY u.state
ORDER BY urbanization_change DESC
```

```response from databricks
[Row(state='Mississippi', urbanization_change=0.10194015502929688), Row(state='Idaho', urbanization_change=0.09984016418457031), Row(state='Vermont', urbanization_change=0.0987405776977539), Row(state='North Dakota', urbanization_change=0.09718990325927734), Row(state='Alaska', urbanization_change=0.08447933197021484), Row(state='Montana', urbanization_change=0.08002948760986328), Row(state='Virginia', urbanization_change=0.07340049743652344), Row(state='Arkansas', urbanization_change=0.07086944580078125), Row(state='Maryland', urbanization_change=0.06741046905517578), Row(state='Nebraska', urbanization_change=0.06509017944335938), Row(state='Wyoming', urbanization_change=0.06394004821777344), Row(state='New Hampshire', urbanization_change=0.05411052703857422), Row(state='West Virginia', urbanization_change=0.052089691162109375), Row(state='Delaware', urbanization_change=0.05056953430175781), Row(state='Oklahoma', urbanization_change=0.047410011291503906), Row(state='Utah', urbanization_change=0.043529510498046875), Row(state='Missouri', urbanization_change=0.04297065734863281), Row(state='New Mexico', urbanization_change=0.042530059814453125), Row(state='Alabama', urbanization_change=0.03914928436279297), Row(state='Indiana', urbanization_change=0.038359642028808594), Row(state='North Carolina', urbanization_change=0.037789344787597656), Row(state='Maine', urbanization_change=0.036789894104003906), Row(state='Oregon', urbanization_change=0.03319072723388672), Row(state='Kansas', urbanization_change=0.03302001953125), Row(state='Colorado', urbanization_change=0.03252983093261719), Row(state='New Jersey', urbanization_change=0.02949047088623047), Row(state='Arizona', urbanization_change=0.028079986572265625), Row(state='Texas', urbanization_change=0.027910232543945312), Row(state='District of Columbia', urbanization_change=0.026940345764160156), Row(state='Puerto Rico', urbanization_change=0.024970054626464844), Row(state='Louisiana', urbanization_change=0.02369976043701172), Row(state='Tennessee', urbanization_change=0.022619247436523438), Row(state='Pennsylvania', urbanization_change=0.02227020263671875), Row(state='Iowa', urbanization_change=0.020209312438964844), Row(state='Georgia', urbanization_change=0.019989967346191406), Row(state='Kentucky', urbanization_change=0.01781940460205078), Row(state='Minnesota', urbanization_change=0.016719818115234375), Row(state='Ohio', urbanization_change=0.016630172729492188), Row(state='South Dakota', urbanization_change=0.016340255737304688), Row(state='Connecticut', urbanization_change=0.016340255737304688), Row(state='Washington', urbanization_change=0.015540122985839844), Row(state='Florida', urbanization_change=0.014650344848632812), Row(state='California', urbanization_change=0.01251983642578125), Row(state='New York', urbanization_change=0.012399673461914062), Row(state='Wisconsin', urbanization_change=0.011980056762695312), Row(state='Michigan', urbanization_change=0.011960029602050781), Row(state='Hawaii', urbanization_change=0.010509490966796875), Row(state='South Carolina', urbanization_change=0.009770393371582031), Row(state='Rhode Island', urbanization_change=0.009610176086425781), Row(state='Nevada', urbanization_change=0.00946044921875), Row(state='Massachusetts', urbanization_change=0.009270668029785156), Row(state='Illinois', urbanization_change=0.007419586181640625)]
```

```sql
SELECT u.state, 
       (MAX(u.urbanindex) - MIN(u.urbanindex)) AS urbanization_change
FROM default.urbanizationdb_tt284 u
GROUP BY u.state
ORDER BY urbanization_change DESC
```

```response from databricks
[Row(state='Mississippi', urbanization_change=0.10194015502929688), Row(state='Idaho', urbanization_change=0.09984016418457031), Row(state='Vermont', urbanization_change=0.0987405776977539), Row(state='North Dakota', urbanization_change=0.09718990325927734), Row(state='Alaska', urbanization_change=0.08447933197021484), Row(state='Montana', urbanization_change=0.08002948760986328), Row(state='Virginia', urbanization_change=0.07340049743652344), Row(state='Arkansas', urbanization_change=0.07086944580078125), Row(state='Maryland', urbanization_change=0.06741046905517578), Row(state='Nebraska', urbanization_change=0.06509017944335938), Row(state='Wyoming', urbanization_change=0.06394004821777344), Row(state='New Hampshire', urbanization_change=0.05411052703857422), Row(state='West Virginia', urbanization_change=0.052089691162109375), Row(state='Delaware', urbanization_change=0.05056953430175781), Row(state='Oklahoma', urbanization_change=0.047410011291503906), Row(state='Utah', urbanization_change=0.043529510498046875), Row(state='Missouri', urbanization_change=0.04297065734863281), Row(state='New Mexico', urbanization_change=0.042530059814453125), Row(state='Alabama', urbanization_change=0.03914928436279297), Row(state='Indiana', urbanization_change=0.038359642028808594), Row(state='North Carolina', urbanization_change=0.037789344787597656), Row(state='Maine', urbanization_change=0.036789894104003906), Row(state='Oregon', urbanization_change=0.03319072723388672), Row(state='Kansas', urbanization_change=0.03302001953125), Row(state='Colorado', urbanization_change=0.03252983093261719), Row(state='New Jersey', urbanization_change=0.02949047088623047), Row(state='Arizona', urbanization_change=0.028079986572265625), Row(state='Texas', urbanization_change=0.027910232543945312), Row(state='District of Columbia', urbanization_change=0.026940345764160156), Row(state='Puerto Rico', urbanization_change=0.024970054626464844), Row(state='Louisiana', urbanization_change=0.02369976043701172), Row(state='Tennessee', urbanization_change=0.022619247436523438), Row(state='Pennsylvania', urbanization_change=0.02227020263671875), Row(state='Iowa', urbanization_change=0.020209312438964844), Row(state='Georgia', urbanization_change=0.019989967346191406), Row(state='Kentucky', urbanization_change=0.01781940460205078), Row(state='Minnesota', urbanization_change=0.016719818115234375), Row(state='Ohio', urbanization_change=0.016630172729492188), Row(state='South Dakota', urbanization_change=0.016340255737304688), Row(state='Connecticut', urbanization_change=0.016340255737304688), Row(state='Washington', urbanization_change=0.015540122985839844), Row(state='Florida', urbanization_change=0.014650344848632812), Row(state='California', urbanization_change=0.01251983642578125), Row(state='New York', urbanization_change=0.012399673461914062), Row(state='Wisconsin', urbanization_change=0.011980056762695312), Row(state='Michigan', urbanization_change=0.011960029602050781), Row(state='Hawaii', urbanization_change=0.010509490966796875), Row(state='South Carolina', urbanization_change=0.009770393371582031), Row(state='Rhode Island', urbanization_change=0.009610176086425781), Row(state='Nevada', urbanization_change=0.00946044921875), Row(state='Massachusetts', urbanization_change=0.009270668029785156), Row(state='Illinois', urbanization_change=0.007419586181640625)]
```

