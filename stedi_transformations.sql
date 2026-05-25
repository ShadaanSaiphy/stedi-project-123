CREATE TABLE customer_trusted AS
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;

CREATE TABLE accelerometer_trusted AS
SELECT a.*
FROM accelerometer_landing a
INNER JOIN customer_trusted c
ON a.user = c.email;

CREATE TABLE customer_curated AS
SELECT DISTINCT c.*
FROM customer_trusted c
INNER JOIN accelerometer_trusted a
ON c.email = a.user;

CREATE TABLE step_trainer_trusted AS
SELECT s.*
FROM step_trainer_landing s
INNER JOIN customer_curated c
ON s.serialnumber = c.serialnumber;

CREATE TABLE machine_learning_curated AS
SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.user,
    a.x,
    a.y,
    a.z
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a
ON s.sensorreadingtime = a.timestamp;