let $encoding=latin1;
let $collation=latin1_bin;
--source illegal_encoding_collation.inc

let $encoding=latin1;
let $collation=latin1_swedish_ci;
--source illegal_encoding_collation.inc

let $encoding=latin1;
let $collation=latin1_general_ci;
--source illegal_encoding_collation.inc

let $encoding=utf8;
let $collation=utf8_unicode_ci;
--source illegal_encoding_collation.inc

let $encoding=utf8;
let $collation=utf8_swedish_ci;
--source illegal_encoding_collation.inc

let $encoding=utf8;
let $collation=utf8_general_ci;
--source illegal_encoding_collation.inc

let $encoding=latin2;
let $collation=latin2_general_ci;
--source illegal_encoding_collation.inc

let $encoding=hebrew;
let $collation=hebrew_general_ci;
--source illegal_encoding_collation.inc

let $encoding=utf16;
let $collation=utf16_general_ci;
--source illegal_encoding_collation.inc

