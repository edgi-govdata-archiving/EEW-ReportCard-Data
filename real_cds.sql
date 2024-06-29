PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE real_cds (
state char(5),
cd tinyint,
unique(state,cd));
INSERT INTO real_cds VALUES('AL',4);
INSERT INTO real_cds VALUES('FL',12);
INSERT INTO real_cds VALUES('GA',2);
INSERT INTO real_cds VALUES('OR',3);
INSERT INTO real_cds VALUES('FL',16);
INSERT INTO real_cds VALUES('IN',8);
INSERT INTO real_cds VALUES('TX',26);
INSERT INTO real_cds VALUES('CA',41);
INSERT INTO real_cds VALUES('IN',7);
INSERT INTO real_cds VALUES('TX',31);
INSERT INTO real_cds VALUES('FL',14);
INSERT INTO real_cds VALUES('CA',28);
INSERT INTO real_cds VALUES('RI',1);
INSERT INTO real_cds VALUES('NY',9);
INSERT INTO real_cds VALUES('MO',5);
INSERT INTO real_cds VALUES('SC',6);
INSERT INTO real_cds VALUES('TN',9);
INSERT INTO real_cds VALUES('OK',4);
INSERT INTO real_cds VALUES('VA',11);
INSERT INTO real_cds VALUES('CA',21);
INSERT INTO real_cds VALUES('CT',2);
INSERT INTO real_cds VALUES('AR',1);
INSERT INTO real_cds VALUES('TX',28);
INSERT INTO real_cds VALUES('IL',7);
INSERT INTO real_cds VALUES('CO',1);
INSERT INTO real_cds VALUES('CT',3);
INSERT INTO real_cds VALUES('TN',4);
INSERT INTO real_cds VALUES('FL',26);
INSERT INTO real_cds VALUES('TX',37);
INSERT INTO real_cds VALUES('SC',3);
INSERT INTO real_cds VALUES('CA',16);
INSERT INTO real_cds VALUES('TN',3);
INSERT INTO real_cds VALUES('NC',5);
INSERT INTO real_cds VALUES('CA',8);
INSERT INTO real_cds VALUES('AZ',9);
INSERT INTO real_cds VALUES('TX',12);
INSERT INTO real_cds VALUES('MO',6);
INSERT INTO real_cds VALUES('TX',9);
INSERT INTO real_cds VALUES('VA',9);
INSERT INTO real_cds VALUES('AZ',7);
INSERT INTO real_cds VALUES('KY',2);
INSERT INTO real_cds VALUES('MD',1);
INSERT INTO real_cds VALUES('NY',26);
INSERT INTO real_cds VALUES('CT',4);
INSERT INTO real_cds VALUES('MD',5);
INSERT INTO real_cds VALUES('MI',4);
INSERT INTO real_cds VALUES('TX',18);
INSERT INTO real_cds VALUES('OH',6);
INSERT INTO real_cds VALUES('GA',4);
INSERT INTO real_cds VALUES('OH',4);
INSERT INTO real_cds VALUES('OH',9);
INSERT INTO real_cds VALUES('MA',9);
INSERT INTO real_cds VALUES('PA',16);
INSERT INTO real_cds VALUES('CO',5);
INSERT INTO real_cds VALUES('WA',2);
INSERT INTO real_cds VALUES('CT',1);
INSERT INTO real_cds VALUES('OH',5);
INSERT INTO real_cds VALUES('CA',12);
INSERT INTO real_cds VALUES('CA',18);
INSERT INTO real_cds VALUES('OK',3);
INSERT INTO real_cds VALUES('MO',3);
INSERT INTO real_cds VALUES('MA',8);
INSERT INTO real_cds VALUES('CA',7);
INSERT INTO real_cds VALUES('CA',20);
INSERT INTO real_cds VALUES('TX',10);
INSERT INTO real_cds VALUES('CA',5);
INSERT INTO real_cds VALUES('MN',4);
INSERT INTO real_cds VALUES('MA',2);
INSERT INTO real_cds VALUES('NC',10);
INSERT INTO real_cds VALUES('WA',5);
INSERT INTO real_cds VALUES('NY',5);
INSERT INTO real_cds VALUES('WI',4);
INSERT INTO real_cds VALUES('NY',12);
INSERT INTO real_cds VALUES('CA',31);
INSERT INTO real_cds VALUES('MA',1);
INSERT INTO real_cds VALUES('DC',0);
INSERT INTO real_cds VALUES('NJ',6);
INSERT INTO real_cds VALUES('NJ',9);
INSERT INTO real_cds VALUES('CA',11);
INSERT INTO real_cds VALUES('ME',1);
INSERT INTO real_cds VALUES('FL',8);
INSERT INTO real_cds VALUES('IL',5);
INSERT INTO real_cds VALUES('KY',5);
INSERT INTO real_cds VALUES('AL',3);
INSERT INTO real_cds VALUES('MD',2);
INSERT INTO real_cds VALUES('MP',0);
INSERT INTO real_cds VALUES('MD',3);
INSERT INTO real_cds VALUES('LA',1);
INSERT INTO real_cds VALUES('IL',9);
INSERT INTO real_cds VALUES('CA',30);
INSERT INTO real_cds VALUES('AZ',1);
INSERT INTO real_cds VALUES('GA',8);
INSERT INTO real_cds VALUES('GA',13);
INSERT INTO real_cds VALUES('VA',3);
INSERT INTO real_cds VALUES('AL',7);
INSERT INTO real_cds VALUES('CA',32);
INSERT INTO real_cds VALUES('ID',2);
INSERT INTO real_cds VALUES('WA',9);
INSERT INTO real_cds VALUES('NE',3);
INSERT INTO real_cds VALUES('NJ',4);
INSERT INTO real_cds VALUES('CA',38);
INSERT INTO real_cds VALUES('MS',2);
INSERT INTO real_cds VALUES('CA',4);
INSERT INTO real_cds VALUES('PA',15);
INSERT INTO real_cds VALUES('NY',20);
INSERT INTO real_cds VALUES('OH',10);
INSERT INTO real_cds VALUES('NY',7);
INSERT INTO real_cds VALUES('MI',5);
INSERT INTO real_cds VALUES('FL',25);
INSERT INTO real_cds VALUES('CA',43);
INSERT INTO real_cds VALUES('FL',11);
INSERT INTO real_cds VALUES('SC',2);
INSERT INTO real_cds VALUES('FL',24);
INSERT INTO real_cds VALUES('VA',1);
INSERT INTO real_cds VALUES('AR',3);
INSERT INTO real_cds VALUES('NV',2);
INSERT INTO real_cds VALUES('OR',1);
INSERT INTO real_cds VALUES('WA',1);
INSERT INTO real_cds VALUES('KY',4);
INSERT INTO real_cds VALUES('NJ',10);
INSERT INTO real_cds VALUES('IL',11);
INSERT INTO real_cds VALUES('NV',1);
INSERT INTO real_cds VALUES('CA',1);
INSERT INTO real_cds VALUES('CA',2);
INSERT INTO real_cds VALUES('CA',6);
INSERT INTO real_cds VALUES('CA',14);
INSERT INTO real_cds VALUES('CA',26);
INSERT INTO real_cds VALUES('CA',29);
INSERT INTO real_cds VALUES('CA',25);
INSERT INTO real_cds VALUES('CA',39);
INSERT INTO real_cds VALUES('CA',52);
INSERT INTO real_cds VALUES('CA',50);
INSERT INTO real_cds VALUES('FL',22);
INSERT INTO real_cds VALUES('KY',6);
INSERT INTO real_cds VALUES('MI',8);
INSERT INTO real_cds VALUES('MO',2);
INSERT INTO real_cds VALUES('NC',9);
INSERT INTO real_cds VALUES('NH',2);
INSERT INTO real_cds VALUES('NY',6);
INSERT INTO real_cds VALUES('NY',8);
INSERT INTO real_cds VALUES('OH',2);
INSERT INTO real_cds VALUES('OH',3);
INSERT INTO real_cds VALUES('OH',14);
INSERT INTO real_cds VALUES('PA',10);
INSERT INTO real_cds VALUES('PA',8);
INSERT INTO real_cds VALUES('TX',14);
INSERT INTO real_cds VALUES('TX',20);
INSERT INTO real_cds VALUES('TX',25);
INSERT INTO real_cds VALUES('TX',33);
INSERT INTO real_cds VALUES('UT',2);
INSERT INTO real_cds VALUES('WA',6);
INSERT INTO real_cds VALUES('WI',2);
INSERT INTO real_cds VALUES('IL',2);
INSERT INTO real_cds VALUES('MO',8);
INSERT INTO real_cds VALUES('MA',5);
INSERT INTO real_cds VALUES('NJ',1);
INSERT INTO real_cds VALUES('NC',12);
INSERT INTO real_cds VALUES('AL',6);
INSERT INTO real_cds VALUES('AR',2);
INSERT INTO real_cds VALUES('AR',4);
INSERT INTO real_cds VALUES('AZ',3);
INSERT INTO real_cds VALUES('CA',10);
INSERT INTO real_cds VALUES('CA',33);
INSERT INTO real_cds VALUES('CA',36);
INSERT INTO real_cds VALUES('CA',35);
INSERT INTO real_cds VALUES('CO',4);
INSERT INTO real_cds VALUES('GA',1);
INSERT INTO real_cds VALUES('GA',11);
INSERT INTO real_cds VALUES('GA',12);
INSERT INTO real_cds VALUES('IL',12);
INSERT INTO real_cds VALUES('LA',6);
INSERT INTO real_cds VALUES('MA',6);
INSERT INTO real_cds VALUES('MI',2);
INSERT INTO real_cds VALUES('MI',6);
INSERT INTO real_cds VALUES('MN',6);
INSERT INTO real_cds VALUES('NC',7);
INSERT INTO real_cds VALUES('NJ',12);
INSERT INTO real_cds VALUES('NY',21);
INSERT INTO real_cds VALUES('PA',2);
INSERT INTO real_cds VALUES('TX',36);
INSERT INTO real_cds VALUES('VA',8);
INSERT INTO real_cds VALUES('VI',0);
INSERT INTO real_cds VALUES('WA',4);
INSERT INTO real_cds VALUES('WI',6);
INSERT INTO real_cds VALUES('WV',2);
INSERT INTO real_cds VALUES('AS',0);
INSERT INTO real_cds VALUES('MS',1);
INSERT INTO real_cds VALUES('IL',16);
INSERT INTO real_cds VALUES('OH',8);
INSERT INTO real_cds VALUES('KY',1);
INSERT INTO real_cds VALUES('PA',3);
INSERT INTO real_cds VALUES('IL',10);
INSERT INTO real_cds VALUES('AZ',5);
INSERT INTO real_cds VALUES('CA',17);
INSERT INTO real_cds VALUES('CA',19);
INSERT INTO real_cds VALUES('CA',24);
INSERT INTO real_cds VALUES('CA',44);
INSERT INTO real_cds VALUES('CA',46);
INSERT INTO real_cds VALUES('DE',0);
INSERT INTO real_cds VALUES('FL',1);
INSERT INTO real_cds VALUES('FL',2);
INSERT INTO real_cds VALUES('FL',5);
INSERT INTO real_cds VALUES('FL',9);
INSERT INTO real_cds VALUES('FL',21);
INSERT INTO real_cds VALUES('GA',3);
INSERT INTO real_cds VALUES('IL',8);
INSERT INTO real_cds VALUES('IN',3);
INSERT INTO real_cds VALUES('LA',3);
INSERT INTO real_cds VALUES('LA',4);
INSERT INTO real_cds VALUES('MD',8);
INSERT INTO real_cds VALUES('MI',1);
INSERT INTO real_cds VALUES('NE',2);
INSERT INTO real_cds VALUES('NJ',5);
INSERT INTO real_cds VALUES('NY',13);
INSERT INTO real_cds VALUES('PA',1);
INSERT INTO real_cds VALUES('PA',11);
INSERT INTO real_cds VALUES('PR',0);
INSERT INTO real_cds VALUES('TN',8);
INSERT INTO real_cds VALUES('TX',34);
INSERT INTO real_cds VALUES('TX',19);
INSERT INTO real_cds VALUES('WA',7);
INSERT INTO real_cds VALUES('WI',8);
INSERT INTO real_cds VALUES('KS',4);
INSERT INTO real_cds VALUES('SC',5);
INSERT INTO real_cds VALUES('CA',34);
INSERT INTO real_cds VALUES('UT',3);
INSERT INTO real_cds VALUES('AZ',8);
INSERT INTO real_cds VALUES('TX',27);
INSERT INTO real_cds VALUES('OH',12);
INSERT INTO real_cds VALUES('OK',1);
INSERT INTO real_cds VALUES('NY',25);
INSERT INTO real_cds VALUES('PA',5);
INSERT INTO real_cds VALUES('PA',7);
INSERT INTO real_cds VALUES('HI',1);
INSERT INTO real_cds VALUES('NV',4);
INSERT INTO real_cds VALUES('AZ',4);
INSERT INTO real_cds VALUES('CA',9);
INSERT INTO real_cds VALUES('CA',47);
INSERT INTO real_cds VALUES('CA',49);
INSERT INTO real_cds VALUES('CO',2);
INSERT INTO real_cds VALUES('CO',6);
INSERT INTO real_cds VALUES('CT',5);
INSERT INTO real_cds VALUES('FL',6);
INSERT INTO real_cds VALUES('FL',17);
INSERT INTO real_cds VALUES('GA',7);
INSERT INTO real_cds VALUES('ID',1);
INSERT INTO real_cds VALUES('IL',4);
INSERT INTO real_cds VALUES('IL',6);
INSERT INTO real_cds VALUES('IL',14);
INSERT INTO real_cds VALUES('IN',4);
INSERT INTO real_cds VALUES('IN',6);
INSERT INTO real_cds VALUES('KS',3);
INSERT INTO real_cds VALUES('MA',3);
INSERT INTO real_cds VALUES('MA',7);
INSERT INTO real_cds VALUES('MD',6);
INSERT INTO real_cds VALUES('MI',7);
INSERT INTO real_cds VALUES('MI',11);
INSERT INTO real_cds VALUES('MI',12);
INSERT INTO real_cds VALUES('MN',2);
INSERT INTO real_cds VALUES('MN',3);
INSERT INTO real_cds VALUES('MN',5);
INSERT INTO real_cds VALUES('MN',8);
INSERT INTO real_cds VALUES('MS',3);
INSERT INTO real_cds VALUES('ND',0);
INSERT INTO real_cds VALUES('NH',1);
INSERT INTO real_cds VALUES('NJ',2);
INSERT INTO real_cds VALUES('NJ',3);
INSERT INTO real_cds VALUES('NJ',11);
INSERT INTO real_cds VALUES('NV',3);
INSERT INTO real_cds VALUES('NY',14);
INSERT INTO real_cds VALUES('PA',4);
INSERT INTO real_cds VALUES('PA',6);
INSERT INTO real_cds VALUES('PA',9);
INSERT INTO real_cds VALUES('PA',13);
INSERT INTO real_cds VALUES('PA',14);
INSERT INTO real_cds VALUES('SC',4);
INSERT INTO real_cds VALUES('SD',0);
INSERT INTO real_cds VALUES('TN',2);
INSERT INTO real_cds VALUES('TN',6);
INSERT INTO real_cds VALUES('TN',7);
INSERT INTO real_cds VALUES('TX',2);
INSERT INTO real_cds VALUES('TX',5);
INSERT INTO real_cds VALUES('TX',7);
INSERT INTO real_cds VALUES('TX',16);
INSERT INTO real_cds VALUES('TX',21);
INSERT INTO real_cds VALUES('TX',29);
INSERT INTO real_cds VALUES('TX',32);
INSERT INTO real_cds VALUES('VA',6);
INSERT INTO real_cds VALUES('VA',7);
INSERT INTO real_cds VALUES('VA',10);
INSERT INTO real_cds VALUES('WA',8);
INSERT INTO real_cds VALUES('WI',1);
INSERT INTO real_cds VALUES('WV',1);
INSERT INTO real_cds VALUES('ME',2);
INSERT INTO real_cds VALUES('NC',8);
INSERT INTO real_cds VALUES('NC',3);
INSERT INTO real_cds VALUES('MD',7);
INSERT INTO real_cds VALUES('WI',7);
INSERT INTO real_cds VALUES('CA',27);
INSERT INTO real_cds VALUES('CA',48);
INSERT INTO real_cds VALUES('TX',17);
INSERT INTO real_cds VALUES('CA',22);
INSERT INTO real_cds VALUES('AL',1);
INSERT INTO real_cds VALUES('AL',2);
INSERT INTO real_cds VALUES('CA',23);
INSERT INTO real_cds VALUES('CA',40);
INSERT INTO real_cds VALUES('CA',45);
INSERT INTO real_cds VALUES('CA',51);
INSERT INTO real_cds VALUES('CO',3);
INSERT INTO real_cds VALUES('FL',3);
INSERT INTO real_cds VALUES('FL',18);
INSERT INTO real_cds VALUES('FL',19);
INSERT INTO real_cds VALUES('FL',28);
INSERT INTO real_cds VALUES('FL',27);
INSERT INTO real_cds VALUES('GA',5);
INSERT INTO real_cds VALUES('GA',9);
INSERT INTO real_cds VALUES('GA',14);
INSERT INTO real_cds VALUES('IA',2);
INSERT INTO real_cds VALUES('IA',1);
INSERT INTO real_cds VALUES('IA',4);
INSERT INTO real_cds VALUES('IL',15);
INSERT INTO real_cds VALUES('IN',1);
INSERT INTO real_cds VALUES('IN',5);
INSERT INTO real_cds VALUES('KS',1);
INSERT INTO real_cds VALUES('KS',2);
INSERT INTO real_cds VALUES('MA',4);
INSERT INTO real_cds VALUES('MI',9);
INSERT INTO real_cds VALUES('MN',7);
INSERT INTO real_cds VALUES('MO',1);
INSERT INTO real_cds VALUES('MT',2);
INSERT INTO real_cds VALUES('NC',2);
INSERT INTO real_cds VALUES('NC',6);
INSERT INTO real_cds VALUES('NM',3);
INSERT INTO real_cds VALUES('NY',2);
INSERT INTO real_cds VALUES('NY',11);
INSERT INTO real_cds VALUES('NY',15);
INSERT INTO real_cds VALUES('NY',16);
INSERT INTO real_cds VALUES('OK',5);
INSERT INTO real_cds VALUES('OR',2);
INSERT INTO real_cds VALUES('SC',1);
INSERT INTO real_cds VALUES('TN',1);
INSERT INTO real_cds VALUES('TX',4);
INSERT INTO real_cds VALUES('TX',11);
INSERT INTO real_cds VALUES('TX',13);
INSERT INTO real_cds VALUES('TX',22);
INSERT INTO real_cds VALUES('TX',23);
INSERT INTO real_cds VALUES('TX',24);
INSERT INTO real_cds VALUES('UT',1);
INSERT INTO real_cds VALUES('UT',4);
INSERT INTO real_cds VALUES('VA',5);
INSERT INTO real_cds VALUES('WA',10);
INSERT INTO real_cds VALUES('WI',5);
INSERT INTO real_cds VALUES('NY',24);
INSERT INTO real_cds VALUES('LA',5);
INSERT INTO real_cds VALUES('LA',2);
INSERT INTO real_cds VALUES('NM',1);
INSERT INTO real_cds VALUES('TX',6);
INSERT INTO real_cds VALUES('OH',11);
INSERT INTO real_cds VALUES('OH',15);
INSERT INTO real_cds VALUES('FL',20);
INSERT INTO real_cds VALUES('NE',1);
INSERT INTO real_cds VALUES('MN',1);
INSERT INTO real_cds VALUES('AK',0);
INSERT INTO real_cds VALUES('NY',18);
INSERT INTO real_cds VALUES('IN',2);
INSERT INTO real_cds VALUES('MT',1);
INSERT INTO real_cds VALUES('AL',5);
INSERT INTO real_cds VALUES('AZ',2);
INSERT INTO real_cds VALUES('AZ',6);
INSERT INTO real_cds VALUES('CA',3);
INSERT INTO real_cds VALUES('CA',13);
INSERT INTO real_cds VALUES('CA',15);
INSERT INTO real_cds VALUES('CA',37);
INSERT INTO real_cds VALUES('CA',42);
INSERT INTO real_cds VALUES('CO',7);
INSERT INTO real_cds VALUES('CO',8);
INSERT INTO real_cds VALUES('FL',4);
INSERT INTO real_cds VALUES('FL',7);
INSERT INTO real_cds VALUES('FL',10);
INSERT INTO real_cds VALUES('FL',13);
INSERT INTO real_cds VALUES('FL',15);
INSERT INTO real_cds VALUES('FL',23);
INSERT INTO real_cds VALUES('GA',6);
INSERT INTO real_cds VALUES('GA',10);
INSERT INTO real_cds VALUES('GU',0);
INSERT INTO real_cds VALUES('HI',2);
INSERT INTO real_cds VALUES('IA',3);
INSERT INTO real_cds VALUES('IL',1);
INSERT INTO real_cds VALUES('IL',3);
INSERT INTO real_cds VALUES('IL',13);
INSERT INTO real_cds VALUES('IL',17);
INSERT INTO real_cds VALUES('IN',9);
INSERT INTO real_cds VALUES('KY',3);
INSERT INTO real_cds VALUES('MD',4);
INSERT INTO real_cds VALUES('MI',3);
INSERT INTO real_cds VALUES('MI',10);
INSERT INTO real_cds VALUES('MI',13);
INSERT INTO real_cds VALUES('MO',4);
INSERT INTO real_cds VALUES('MO',7);
INSERT INTO real_cds VALUES('MS',4);
INSERT INTO real_cds VALUES('NC',1);
INSERT INTO real_cds VALUES('NC',4);
INSERT INTO real_cds VALUES('NC',11);
INSERT INTO real_cds VALUES('NC',13);
INSERT INTO real_cds VALUES('NC',14);
INSERT INTO real_cds VALUES('NJ',7);
INSERT INTO real_cds VALUES('NJ',8);
INSERT INTO real_cds VALUES('NM',2);
INSERT INTO real_cds VALUES('NY',1);
INSERT INTO real_cds VALUES('NY',3);
INSERT INTO real_cds VALUES('NY',4);
INSERT INTO real_cds VALUES('NY',10);
INSERT INTO real_cds VALUES('NY',17);
INSERT INTO real_cds VALUES('NY',19);
INSERT INTO real_cds VALUES('NY',22);
INSERT INTO real_cds VALUES('NY',23);
INSERT INTO real_cds VALUES('OH',1);
INSERT INTO real_cds VALUES('OH',7);
INSERT INTO real_cds VALUES('OH',13);
INSERT INTO real_cds VALUES('OK',2);
INSERT INTO real_cds VALUES('OR',4);
INSERT INTO real_cds VALUES('OR',5);
INSERT INTO real_cds VALUES('OR',6);
INSERT INTO real_cds VALUES('PA',12);
INSERT INTO real_cds VALUES('PA',17);
INSERT INTO real_cds VALUES('RI',2);
INSERT INTO real_cds VALUES('SC',7);
INSERT INTO real_cds VALUES('TN',5);
INSERT INTO real_cds VALUES('TX',1);
INSERT INTO real_cds VALUES('TX',3);
INSERT INTO real_cds VALUES('TX',8);
INSERT INTO real_cds VALUES('TX',15);
INSERT INTO real_cds VALUES('TX',30);
INSERT INTO real_cds VALUES('TX',35);
INSERT INTO real_cds VALUES('TX',38);
INSERT INTO real_cds VALUES('VA',2);
INSERT INTO real_cds VALUES('VT',0);
INSERT INTO real_cds VALUES('WA',3);
INSERT INTO real_cds VALUES('WI',3);
INSERT INTO real_cds VALUES('WY',0);
INSERT INTO real_cds VALUES('VA',4);
COMMIT;