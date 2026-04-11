-- Migration: 0001_backfill_referred_by
-- Backfill the referred_by column for mentors whose referral data was missing
-- at the time of insertion. Each UPDATE is idempotent — it only patches rows
-- where referred_by is currently NULL or empty string.
-- Closes: https://github.com/OWASP-BLT/BLT-Pool/issues/52

UPDATE mentors
SET referred_by = 'mdkaifansari04'
WHERE lower(github_username) = 'rinkitadhana'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'mdkaifansari04'
WHERE lower(github_username) = 'rajgupta36'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'arnavkirti'
WHERE lower(github_username) = 'shriyashsoni'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'arnavkirti'
WHERE lower(github_username) = 'mohammedfaiyaz29'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Pritz395'
WHERE lower(github_username) = 'vaswani2003'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Pritz395'
WHERE lower(github_username) = 'kittenbytes'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'e-esakman'
WHERE lower(github_username) = 'captain-t2004'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'S3DFX-CYBER'
WHERE lower(github_username) = 'elsheik21'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'S3DFX-CYBER'
WHERE lower(github_username) = 'kunal1522'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Rudra-rps'
WHERE lower(github_username) = 'rudrabhaskar9439'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Rudra-rps'
WHERE lower(github_username) = 'dev-sanidhya'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'sidd190'
WHERE lower(github_username) = 'vedantanand17'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'sidd190'
WHERE lower(github_username) = 'rishab87'
  AND (referred_by IS NULL OR referred_by = '');
