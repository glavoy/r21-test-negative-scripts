-- Fix the swapped MRC values and corresponding SUBJID
UPDATE enrollee
SET 
    -- Update the subjid by replacing the 4th, 5th, and 6th digits
    subjid = CASE 
                WHEN mrc = '096' THEN STUFF(subjid, 4, 3, '110')
                WHEN mrc = '110' THEN STUFF(subjid, 4, 3, '096')
                ELSE subjid
             END,
    -- Update the mrc field
    mrc = CASE 
            WHEN mrc = '096' THEN '110'
            WHEN mrc = '110' THEN '096'
            ELSE mrc
          END
WHERE survey_id = 'r21_test_negative_2026-01-05'
  AND mrc IN ('096', '110');
  
  
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



-- fix duplicate subjid's - these were cause in version 2026-02-02 since we did not change the deviceid, but rather the survey id - by mistake.
-- this was initially caused by using the incorrect mrc codes and then changing them in the code above, and then changing them 
-- to eb the correct ones in new data dictionary.
update enrollee set deviceid = '802', deviceid2 = '802', subjid = '8020960001' where uniqueid = 'c1ee0796-f9dd-49b1-a077-fa407a7b72bf'
update enrollee set deviceid = '802', deviceid2 = '802', subjid = '8020960002' where uniqueid = '21478b4b-9323-4a79-b5da-e4e74f2e518e'