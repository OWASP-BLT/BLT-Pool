-- Migration: 0001_backfill_referred_by
-- Backfill referred_by for mentors whose referral data was missing at insertion.
-- Each UPDATE is idempotent (only patches rows where referred_by IS NULL or empty).
-- Multiple mentors per contributor are stored in contributor_referrals, one row
-- per mentor-contributor pair. referred_by stores the primary (first) mentor only.
--
-- Closes: https://github.com/OWASP-BLT/BLT-Pool/issues/52
--
-- ── Part A: Original 13 mentor → referrer pairs ─────────────────────────────
-- mentor              referred_by
-- rinkitadhana        mdkaifansari04
-- rajgupta36          mdkaifansari04
-- shriyashsoni        arnavkirti
-- mohammedfaiyaz29    arnavkirti
-- vaswani2003         pritz395
-- kittenbytes         pritz395
-- captain-t2004       e-esakman
-- elsheik21           s3dfx-cyber
-- kunal1522           s3dfx-cyber
-- rudrabhaskar9439    rudra-rps
-- dev-sanidhya        rudra-rps
-- vedantanand17       sidd190
-- rishab87            sidd190

UPDATE mentors SET referred_by = 'mdkaifansari04'
WHERE lower(github_username) = 'rinkitadhana' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'mdkaifansari04'
WHERE lower(github_username) = 'rajgupta36' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'arnavkirti'
WHERE lower(github_username) = 'shriyashsoni' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'arnavkirti'
WHERE lower(github_username) = 'mohammedfaiyaz29' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'pritz395'
WHERE lower(github_username) = 'vaswani2003' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'pritz395'
WHERE lower(github_username) = 'kittenbytes' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'e-esakman'
WHERE lower(github_username) = 'captain-t2004' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 's3dfx-cyber'
WHERE lower(github_username) = 'elsheik21' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 's3dfx-cyber'
WHERE lower(github_username) = 'kunal1522' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'rudra-rps'
WHERE lower(github_username) = 'rudrabhaskar9439' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'rudra-rps'
WHERE lower(github_username) = 'dev-sanidhya' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'sidd190'
WHERE lower(github_username) = 'vedantanand17' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'sidd190'
WHERE lower(github_username) = 'rishab87' AND (referred_by IS NULL OR referred_by = '');

-- ── Part B: Contributor checklist — 13 assignees (Apr 2026) ─────────────────
-- referred_by = primary (first) mentor only; all mentors stored in
-- contributor_referrals below (one row per mentor-contributor pair).
--
-- assignee           primary mentor       all mentors                          PRs
-- preetham           vinamra              vinamra, carla                       39
-- mdkaifansari04     raj                  raj, rinkitadhana                    52
-- ananya             shirshjain           shirshjain, ankitsinghsisodya        19
-- saviod             kunal1522            kunal1522, chigorin                  20
-- mohammedaashik     manikandanchandran   manikandanchandran                   —
-- jayant             manikandanchandran   manikandanchandran, bishaldas        —
-- shazzahrazaidi     rehan                rehan                                8
-- sidd190            rishab87             rishab87, vedantanand17              30
-- rosai              jisan                jisan, shivam                        70
-- shubhangpathak     aadityasharma        aadityasharma                        —
-- sakshee            akshaybehl           akshaybehl, arpitchaudhary,          28
--                                         vedantanand17, sakshamgupta
-- arnavkirti         shriyashsoni         shriyashsoni, mohammedfaiyaz29,      —
--                                         mariyan-zarev, tanishtyagi, ankitshankar
-- shivanandu         kunal1522            kunal1522, aryanjain                 1

UPDATE mentors SET referred_by = 'vinamra'
WHERE lower(github_username) = 'preetham' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'raj'
WHERE lower(github_username) = 'mdkaifansari04' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'shirshjain'
WHERE lower(github_username) = 'ananya' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'kunal1522'
WHERE lower(github_username) = 'saviod' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'manikandanchandran'
WHERE lower(github_username) = 'mohammedaashik' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'manikandanchandran'
WHERE lower(github_username) = 'jayant' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'rehan'
WHERE lower(github_username) = 'shazzahrazaidi' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'rishab87'
WHERE lower(github_username) = 'sidd190' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'jisan'
WHERE lower(github_username) = 'rosai' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'aadityasharma'
WHERE lower(github_username) = 'shubhangpathak' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'akshaybehl'
WHERE lower(github_username) = 'sakshee' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'shriyashsoni'
WHERE lower(github_username) = 'arnavkirti' AND (referred_by IS NULL OR referred_by = '');

UPDATE mentors SET referred_by = 'kunal1522'
WHERE lower(github_username) = 'shivanandu' AND (referred_by IS NULL OR referred_by = '');

-- ── contributor_referrals: one row per mentor-contributor pair ───────────────
-- All logins lowercase to match runtime normalization.
-- Explicit conflict target matches the PRIMARY KEY definition.

-- 1. preetham ← vinamra, carla
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('vinamra'), lower('preetham'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('carla'), lower('preetham'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 2. mdkaifansari04 ← raj, rinkitadhana
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('raj'), lower('mdkaifansari04'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('rinkitadhana'), lower('mdkaifansari04'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 3. ananya ← shirshjain, ankitsinghsisodya
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('shirshjain'), lower('ananya'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('ankitsinghsisodya'), lower('ananya'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 4. saviod ← kunal1522, chigorin
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('kunal1522'), lower('saviod'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('chigorin'), lower('saviod'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 5. mohammedaashik ← manikandanchandran
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('manikandanchandran'), lower('mohammedaashik'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 6. jayant ← manikandanchandran, bishaldas
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('manikandanchandran'), lower('jayant'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('bishaldas'), lower('jayant'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 7. shazzahrazaidi ← rehan
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('rehan'), lower('shazzahrazaidi'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 8. sidd190 ← rishab87, vedantanand17
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('rishab87'), lower('sidd190'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('vedantanand17'), lower('sidd190'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 9. rosai ← jisan, shivam
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('jisan'), lower('rosai'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('shivam'), lower('rosai'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 10. shubhangpathak ← aadityasharma
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('aadityasharma'), lower('shubhangpathak'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 11. sakshee ← akshaybehl, arpitchaudhary, vedantanand17, sakshamgupta
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('akshaybehl'), lower('sakshee'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('arpitchaudhary'), lower('sakshee'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('vedantanand17'), lower('sakshee'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('sakshamgupta'), lower('sakshee'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 12. arnavkirti ← shriyashsoni, mohammedfaiyaz29, mariyan-zarev, tanishtyagi, ankitshankar
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('shriyashsoni'), lower('arnavkirti'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('mohammedfaiyaz29'), lower('arnavkirti'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('mariyan-zarev'), lower('arnavkirti'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('tanishtyagi'), lower('arnavkirti'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('ankitshankar'), lower('arnavkirti'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

-- 13. shivanandu ← kunal1522, aryanjain
INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('kunal1522'), lower('shivanandu'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;

INSERT INTO contributor_referrals (org, month_key, referrer_login, referred_login, repo, issue_number, created_at)
VALUES ('OWASP-BLT', '2026-04', lower('aryanjain'), lower('shivanandu'), 'BLT-Pool', 0, 1744329600)
ON CONFLICT (org, month_key, referrer_login, referred_login) DO NOTHING;
