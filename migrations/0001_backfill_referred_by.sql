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

UPDATE mentors
SET referred_by = 'Vinamra'
WHERE lower(github_username) = 'preetham'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'raj'
WHERE lower(github_username) = 'mdkaifansari04'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'ShirshJain'
WHERE lower(github_username) = 'ananya'
  AND (referred_by IS NULL OR referred_by = '');

-- Savio D'souza → Kunal (primary mentor)
UPDATE mentors
SET referred_by = 'Kunal1522'
WHERE lower(github_username) = 'saviod'
  AND (referred_by IS NULL OR referred_by = '');

-- Mohammed Aashik → Manikandan Chandran
UPDATE mentors
SET referred_by = 'ManikandanChandran'
WHERE lower(github_username) = 'mohammedaashik'
  AND (referred_by IS NULL OR referred_by = '');

-- Jayant → Manikandan Chandran (primary mentor)
UPDATE mentors
SET referred_by = 'ManikandanChandran'
WHERE lower(github_username) = 'jayant'
  AND (referred_by IS NULL OR referred_by = '');

-- Shaz Zahra Zaidi → Rehan
UPDATE mentors
SET referred_by = 'Rehan'
WHERE lower(github_username) = 'shazzahrazaidi'
  AND (referred_by IS NULL OR referred_by = '');

-- Siddharth Bansal → Rishab Kumar Jha (primary mentor)
UPDATE mentors
SET referred_by = 'Rishab87'
WHERE lower(github_username) = 'sidd190'
  AND (referred_by IS NULL OR referred_by = '');

-- Rosai → Jisan (primary mentor)
UPDATE mentors
SET referred_by = 'Jisan'
WHERE lower(github_username) = 'rosai'
  AND (referred_by IS NULL OR referred_by = '');

-- Shubhang pathak → Aaditya Sharma
UPDATE mentors
SET referred_by = 'AadityaSharma'
WHERE lower(github_username) = 'shubhangpathak'
  AND (referred_by IS NULL OR referred_by = '');

-- Sakshee → Akshay Behl (primary mentor)
UPDATE mentors
SET referred_by = 'AkshayBehl'
WHERE lower(github_username) = 'sakshee'
  AND (referred_by IS NULL OR referred_by = '');

-- Arnav Kirti → shriyash soni (primary mentor)
UPDATE mentors
SET referred_by = 'shriyashsoni'
WHERE lower(github_username) = 'arnavkirti'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Kunal1522'
WHERE lower(github_username) = 'shivanandu'
  AND (referred_by IS NULL OR referred_by = '');
