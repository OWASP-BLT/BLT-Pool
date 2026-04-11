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

UPDATE mentors
SET referred_by = 'Kunal1522'
WHERE lower(github_username) = 'saviod'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'ManikandanChandran'
WHERE lower(github_username) = 'mohammedaashik'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'ManikandanChandran'
WHERE lower(github_username) = 'jayant'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Rehan'
WHERE lower(github_username) = 'shazzahrazaidi'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Rishab87'
WHERE lower(github_username) = 'sidd190'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Jisan'
WHERE lower(github_username) = 'rosai'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'AadityaSharma'
WHERE lower(github_username) = 'shubhangpathak'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'AkshayBehl'
WHERE lower(github_username) = 'sakshee'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'shriyashsoni'
WHERE lower(github_username) = 'arnavkirti'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Kunal1522'
WHERE lower(github_username) = 'shivanandu'
  AND (referred_by IS NULL OR referred_by = '');


INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'vinamra', 'preetham', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'carla', 'preetham', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'raj', 'mdkaifansari04', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'rinkitadhana', 'mdkaifansari04', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'shirshjain', 'ananya', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'ankitsinghsisodya', 'ananya', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'kunal1522', 'saviod', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'chigorin', 'saviod', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'manikandanchandran', 'mohammedaashik', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'manikandanchandran', 'jayant', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'bishaldas', 'jayant', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'rehan', 'shazzahrazaidi', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'rishab87', 'sidd190', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'vedantanand17', 'sidd190', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'jisan', 'rosai', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'shivam', 'rosai', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'aadityasharma', 'shubhangpathak', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'akshaybehl', 'sakshee', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'arpitchaudhary', 'sakshee', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'vedantanand17', 'sakshee', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'sakshamgupta', 'sakshee', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

-- 12. Arnav Kirti ← Shriyash Soni, Faiyaz, Mariyan Zarev, Tanish Tyagi, Ankit Shankar
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'shriyashsoni', 'arnavkirti', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'mohammedfaiyaz29', 'arnavkirti', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'mariyan-zarev', 'arnavkirti', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'tanishtyagi', 'arnavkirti', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'ankitshankar', 'arnavkirti', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'kunal1522', 'shivanandu', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', 'aryanjain', 'shivanandu', 'BLT-Pool', 0, 1744329600)
ON CONFLICT DO NOTHING;

UPDATE mentors
SET referred_by = 'Vinamra,Carla'
WHERE lower(github_username) = 'preetham'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'raj,rinkitadhana'
WHERE lower(github_username) = 'mdkaifansari04'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'ShirshJain,AnkitSinghSisodya'
WHERE lower(github_username) = 'ananya'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Kunal1522,Chigorin'
WHERE lower(github_username) = 'saviod'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'ManikandanChandran'
WHERE lower(github_username) = 'mohammedaashik'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'ManikandanChandran,BishalDas'
WHERE lower(github_username) = 'jayant'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Rehan'
WHERE lower(github_username) = 'shazzahrazaidi'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Rishab87,VedantAnand17'
WHERE lower(github_username) = 'sidd190'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Jisan,Shivam'
WHERE lower(github_username) = 'rosai'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'AadityaSharma'
WHERE lower(github_username) = 'shubhangpathak'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'AkshayBehl,ArpitChaudhary,VedantAnand17,SakshamGupta'
WHERE lower(github_username) = 'sakshee'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'shriyashsoni,Mohammedfaiyaz29,MarianZarev,TanishTyagi,AnkitShankar'
WHERE lower(github_username) = 'arnavkirti'
  AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors
SET referred_by = 'Kunal1522,AryanJain'
WHERE lower(github_username) = 'shivanandu'
  AND (referred_by IS NULL OR referred_by = '');
