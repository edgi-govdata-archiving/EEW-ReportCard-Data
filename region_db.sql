CREATE TABLE active_facilities ( region_id tinyint, program char(20), count int,
unique( region_id, program ));
CREATE TABLE regions (
region_type char(30),
state char(10),
region char(20),
unique(region_type,state,region));
CREATE TABLE per_fac (
region_id tinyint,
program char(20),
type char(20),
year tinyint,
count real,
unique(region_id,program,type,year));
CREATE TABLE violations (
region_id tinyint,
program char(20),
year tinyint,
count int,
unique(region_id,program,year));
CREATE TABLE inspections (
region_id tinyint,
program char(20),
year tinyint,
count int,
unique(region_id,program,year));
CREATE TABLE recurring_violations (
region_id tinyint,
program char(20),
facilities int,
percent real, violations tinyint,
unique( region_id, program ));
CREATE TABLE enforcements (
region_id tinyint,
program char(20),
year tinyint,
amount double,
count int,
unique( region_id,program,year));
CREATE TABLE ghg_emissions (
region_id tinyint,
year tinyint,
amount real,
unique( region_id, year ));
CREATE TABLE non_compliants(
region_id tinyint,
program char(20),
fac_name char(256),
noncomp_count tinyint,
formal_action_count tinyint,
dfr_url char(256),
fac_lat real,
fac_long real);
CREATE TABLE violations_by_facilities (
region_id tinyint,
program char(20),
noncomp_qtrs tinyint,
num_facilities tinyint,
unique( region_id, program, noncomp_qtrs ));
CREATE TABLE enf_per_fac (
region_id tinyint,
program char(20),
year tinyint,
amount real,
count tinyint,
num_fac tinyint,
unique( region_id, program, year ));
CREATE TABLE inflation (
year tinyint,
rate real );
CREATE TABLE real_cds (
state char(5),
cd tinyint,
unique(state,cd));
CREATE TABLE IF NOT EXISTS "state_per_1000" (
"index" INTEGER,
  "CD.State" TEXT,
  "CAA.Insp.per.1000" REAL,
  "CAA.Viol.per.1000" REAL,
  "CAA.Enf.per.1000" REAL,
  "CWA.Insp.per.1000" REAL,
  "CWA.Viol.per.1000" REAL,
  "CWA.Enf.per.1000" REAL,
  "RCRA.Insp.per.1000" REAL,
  "RCRA.Viol.per.1000" REAL,
  "RCRA.Enf.per.1000" REAL,
  "CAA_Insp_Rank" REAL,
  "CAA_Viol_Rank" REAL,
  "CAA_Enf_Rank" REAL,
  "CWA_Insp_Rank" REAL,
  "CWA_Viol_Rank" REAL,
  "CWA_Enf_Rank" REAL,
  "RCRA_Insp_Rank" REAL,
  "RCRA_Viol_Rank" REAL,
  "RCRA_Enf_Rank" REAL
);
CREATE INDEX "ix_state_per_1000_index"ON "state_per_1000" ("index");
CREATE TABLE IF NOT EXISTS "cd_per_1000" (
"index" INTEGER,
  "CD.State" TEXT,
  "CAA.Insp.per.1000" REAL,
  "CAA.Viol.per.1000" REAL,
  "CAA.Enf.per.1000" REAL,
  "CWA.Insp.per.1000" REAL,
  "CWA.Viol.per.1000" REAL,
  "CWA.Enf.per.1000" REAL,
  "RCRA.Insp.per.1000" REAL,
  "RCRA.Viol.per.1000" REAL,
  "RCRA.Enf.per.1000" REAL,
  "CAA_Insp_Pct" REAL,
  "CAA_Viol_Pct" REAL,
  "CAA_Enf_Pct" REAL,
  "CWA_Insp_Pct" REAL,
  "CWA_Viol_Pct" REAL,
  "CWA_Enf_Pct" REAL,
  "RCRA_Insp_Pct" REAL,
  "RCRA_Viol_Pct" REAL,
  "RCRA_Enf_Pct" REAL
);
CREATE INDEX "ix_cd_per_1000_index"ON "cd_per_1000" ("index");
CREATE TABLE IF NOT EXISTS "county_per_1000" (
"index" INTEGER,
  "CD.State" TEXT,
  "CAA.Insp.per.1000" REAL,
  "CAA.Viol.per.1000" REAL,
  "CAA.Enf.per.1000" REAL,
  "CWA.Insp.per.1000" REAL,
  "CWA.Viol.per.1000" REAL,
  "CWA.Enf.per.1000" REAL,
  "RCRA.Insp.per.1000" REAL,
  "RCRA.Viol.per.1000" REAL,
  "RCRA.Enf.per.1000" REAL,
  "CAA_Insp_Pct" REAL,
  "CAA_Viol_Pct" REAL,
  "CAA_Enf_Pct" REAL,
  "CWA_Insp_Pct" REAL,
  "CWA_Viol_Pct" REAL,
  "CWA_Enf_Pct" REAL,
  "RCRA_Insp_Pct" REAL,
  "RCRA_Viol_Pct" REAL,
  "RCRA_Enf_Pct" REAL
);
CREATE INDEX "ix_county_per_1000_index"ON "county_per_1000" ("index");
CREATE TABLE IF NOT EXISTS "config" (
focus_year tinyint,
unique(focus_year)
);
