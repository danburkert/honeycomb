USE hbase;
DROP TABLE IF EXISTS `Department`;
CREATE TABLE `Department` (
  `Id` int(11) DEFAULT NULL,
  `Name` varchar(50) COLLATE utf8_bin DEFAULT NULL,
  KEY `Id` (`Id`),
  KEY `Name` (`Name`)
) ENGINE=Honeycomb DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
INSERT INTO `Department` VALUES (2,'Quality Control'),(7,'Manufacturing'),(6,'Shipping'),(8,'Maintenance'),(4,'Operations'),(0,'Research & Development'),(3,'Human Resources'),(1,'Legal'),(5,'Business Development'),(9,'Accounting');
