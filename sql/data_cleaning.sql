-- Swap 096/110 mrc codes - these were incorrect in the 'r21_test_negative_2026-01-05' data dictionary

UPDATE enrollee
SET mrc = CASE 
            WHEN mrc = '096' THEN '110'
            WHEN mrc = '110' THEN '096'
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