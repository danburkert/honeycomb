# SIGNED   | NO INDEX | NOT NULL
--source signed.inc
--source noindex.inc
--source notnull.inc
--source numeric.inc

# SIGNED   | NO INDEX | NULLABLE
--source signed.inc
--source noindex.inc
--source nullable.inc
--source nullable_numeric.inc
--source numeric.inc

# SIGNED   | INDEXED  | NOT NULL
--source signed.inc
--source index.inc
--source notnull.inc
--source numeric.inc

# SIGNED   | INDEXED  | NULLABLE
--source signed.inc
--source index.inc
--source nullable.inc
--source nullable_numeric.inc
--source numeric.inc

# UNSIGNED | NO INDEX | NOT NULL
--source unsigned.inc
--source noindex.inc
--source notnull.inc
--source numeric.inc

# UNSIGNED | NO INDEX | NULLABLE
--source unsigned.inc
--source noindex.inc
--source nullable.inc
--source nullable_numeric.inc
--source numeric.inc

# UNSIGNED | INDEXED  | NOT NULL
--source unsigned.inc
--source index.inc
--source notnull.inc
--source numeric.inc

# UNSIGNED | INDEXED  | NULLABLE
--source unsigned.inc
--source index.inc
--source nullable.inc
--source nullable_numeric.inc
--source numeric.inc
