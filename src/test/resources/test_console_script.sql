CREATE SCHEMA tab1 FOR TABLE table1 (
    keyval key,
    f1:val1 string alias val1,
    f1:val2 string alias val2,
    f1:val3 string alias notdefinedval,
    f2:val1 date alias val3,
    f2:val2 date alias val4,
    f3:val1 int alias val5,
    f3:val2 int alias val6,
    f3:val3 int alias val7,
    f3:val4 int[] alias val8,
    f3:mapval1 string mapKeysAsColumns alias f3mapval1,
    f3:mapval2 string mapKeysAsColumns alias f3mapval2
);


select * from tab1;
