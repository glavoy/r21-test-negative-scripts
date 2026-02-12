USE r21_neg


-- create barcode_corrections table
CREATE TABLE barcode_corrections (
    incorrect_barcode VARCHAR(50) PRIMARY KEY,
    correct_barcode VARCHAR(50) NOT NULL
);

-- Insert current list of corrections
INSERT INTO barcode_corrections (incorrect_barcode, correct_barcode)
VALUES 
('R21-096-Q37I', 'R21-096-Q371'),
('R21-096-NYZI', 'R21-096-NYZ1'),
('R21-096-E121', 'R21-096-E12I'),
('R21-096-QL06', 'R21-096-QLO6'),
('R21-096-Z6GI', 'R21-096-Z6G1')
('R21-096-4541', 'R21-096-454I'),
('R21-110-31VT', 'R21-110-3IVT');


--update the barcodes in the enrollee table
UPDATE e
SET 
    e.fpbarcode1_r21 = ISNULL(map1.correct_barcode, e.fpbarcode1_r21),
    e.fpbarcode2_r21 = ISNULL(map2.correct_barcode, e.fpbarcode2_r21),
    e.fpbarcode1_immrse = ISNULL(map3.correct_barcode, e.fpbarcode1_immrse),
    e.fpbarcode2_immrse = ISNULL(map4.correct_barcode, e.fpbarcode2_immrse)
FROM enrollee e
LEFT JOIN barcode_corrections map1 ON e.fpbarcode1_r21 = map1.incorrect_barcode
LEFT JOIN barcode_corrections map2 ON e.fpbarcode2_r21 = map2.incorrect_barcode
LEFT JOIN barcode_corrections map3 ON e.fpbarcode1_immrse = map3.incorrect_barcode
LEFT JOIN barcode_corrections map4 ON e.fpbarcode2_immrse = map4.incorrect_barcode
WHERE map1.incorrect_barcode IS NOT NULL 
   OR map2.incorrect_barcode IS NOT NULL
   OR map3.incorrect_barcode IS NOT NULL
   OR map4.incorrect_barcode IS NOT NULL;


-- view updated barcodes
SELECT 
    e.survey_id,
    e.mrc,
    e.fpbarcode1_r21, 
    e.fpbarcode2_r21, 
    e.fpbarcode1_immrse, 
    e.fpbarcode2_immrse
FROM enrollee e
WHERE EXISTS (
    SELECT 1 FROM barcode_corrections bc 
    WHERE e.fpbarcode1_r21 IN (bc.incorrect_barcode, bc.correct_barcode)
       OR e.fpbarcode2_r21 IN (bc.incorrect_barcode, bc.correct_barcode)
       OR e.fpbarcode1_immrse IN (bc.incorrect_barcode, bc.correct_barcode)
       OR e.fpbarcode2_immrse IN (bc.incorrect_barcode, bc.correct_barcode)
);

select subjid, count(*) from enrollee group by subjid having count(*) > 1
select * from enrollee where subjid = '1011100001'
select * from enrollee where subjid = '1010960089'

select * from formchanges where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'

update enrollee set mrc = '096' where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'
update enrollee set subjid = '1010960089' where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'
update enrollee set swver = 'GiSTX 0.0.8' where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'
update enrollee set survey_id = 'r21_test_negative_2026-01-05' where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'
update enrollee set lastmod = '2026-02-02 15:32:55.010614' where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'
update enrollee set ill_noteligible = NULL where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'
update enrollee set vx_card_no = NULL where uniqueid = '4cf28a97-6efa-486d-93a9-7af8ecca2597'



