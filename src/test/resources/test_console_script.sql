CREATE MAPPING tab1a FOR TABLE table1 (
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
    f3:mapval1 string alias f3mapval1,
    f3:mapval2 string alias f3mapval2
);

CREATE MAPPING tab1b FOR TABLE table1 (
    keyval KEY,
    f1 (
        val1 string alias val1,
        val2 string alias val2,
        val3 string alias notdefinedval
    ),
    f2 (
        val1 date alias val3,
        val2 date alias val4
    ),
    f3 (
        val1 int alias val5,
        val2 int alias val6,
        val3 int alias val7,
        val4 int[] alias val8,
        mapval1 string alias f3mapval1,
        mapval2 string alias f3mapval2
    )
);


select * from tab1b;
