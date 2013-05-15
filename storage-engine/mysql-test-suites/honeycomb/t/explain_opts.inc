let $opts = ;
--source explain_operators.inc

let $opts = LIMIT 10;
--source explain_operators.inc

let $opts = ORDER BY $col ASC;
--source explain_operators.inc

let $opts = ORDER BY $col DESC;
--source explain_operators.inc

let $opts = ORDER BY $col ASC LIMIT 10;
--source explain_operators.inc

let $opts = ORDER BY $col DESC LIMIT 10;
--source explain_operators.inc
