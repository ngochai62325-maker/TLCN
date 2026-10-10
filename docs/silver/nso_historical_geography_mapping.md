# NSO historical geography reviewed proposal ? E1

The 71 distinct raw labels comprise one country label, six region labels and 64 province candidates, including H? T?y. Exact raw labels are preserved. Candidate spelling/level/parent is a human-readable proposal; it is not an approved canonical identity. No fuzzy repair is used, and no current province code is assigned to a historical year.

Machine-review inventory: [JSON](nso_historical_geography_proposal.json) and [CSV](nso_historical_geography_proposal.csv), 750 exact source-table/raw-label pairs. Each entry includes raw label, source table, proposed level/name, code/scheme when verified, parent candidate, valid-from/to, source reference, confidence, status and review note. National code 704 is supported by [UN M49](https://unstats.un.org/unsd/methodology/m49/); its identity verification is distinct from dated province boundary approval. Non-national code, valid-from/to and historical references remain NULL/unresolved. Parent candidates use source row order and require review against historical boundaries.

## Historical boundaries

[NSO overview of the 2008 yearbook](https://www.nso.gov.vn/default/2026/01/nien-giam-thong-ke-2008/) describes the H? N?i expansion effective 1 August 2008 and a change from eight to six socioeconomic regions. This is direct evidence against applying current province/region definitions throughout 1995?2024. It does not by itself supply valid intervals for every raw label. See also [NSO administrative decree catalog](https://danhmuchanhchinh.nso.gov.vn/NghiDinh.aspx). H? T?y and H? N?i transition-year observations require a table-specific statistical-boundary note, not a guess based only on the legal effective date.

Recommended choices: (1) preserve raw labels, (2) approve exact aliases only with historical references, (3) assign a versioned code scheme and full-year validity intervals, (4) keep 2008 transition-year cases needs_review until the reporting boundary is established, (5) review split/merged provinces and regions independently, including the timing of ?i?n Bi?n/Lai Ch?u, ??k N?ng/??k L?k and H?u Giang/C?n Th? without assuming their dates, (6) leave unmatched labels in quarantine with no Gold eligibility. The visibly damaged B? R?a - Vtng T?u spelling has LOW candidate confidence; it is not repaired in the transformer.

Approved runtime mapping file `config/silver/nso_geography_reviewed.json` is deliberately empty. Existing source transformation uses `src/silver/mappings/nso_geography.py`: exact join on source table and raw label; a verified entry must provide code, scheme, canonical name, level, dates and reference. The interval must contain January 1 through December 31 of the reference year. Overlapping entries fail before join, avoiding duplicated observations. NEEDS_APPROVAL entries are ignored. Reviewed codes in tests are clearly synthetic and never written to production configuration.

## Complete 71-label inventory

Source column lists suffixes of V06 CSV filenames. Per-source parents, validity, references and confidence are in the linked 750-row proposal.

| Raw source label | V06 source suffixes | Proposed level | Canonical name candidate | Verified code | Status |
|---|---|---|---|---|---|
| An Giang | 13,14,15,16,17,18,19,20,21,22,23,24 | province | An Giang | ? | needs_review |
| B?c Giang | 13,14,15,16,17,18,22,23,24 | province | Bắc Giang | ? | needs_review |
| B?c K?n | 13,14,15,16,17,18,22,23,24 | province | Bắc Kạn | ? | needs_review |
| B?c Liêu | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Bạc Liêu | ? | needs_review |
| B?c Ninh | 13,14,15,16,17,18,22,23,24 | province | Bắc Ninh | ? | needs_review |
| B?c Trung B? và Duyên h?i mi?n Trung | 13,14,15,16,17,18,19,20,21,22,23,24 | region | Bắc Trung Bộ và Duyên hải miền Trung | ? | needs_review |
| B?n Tre | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Bến Tre | ? | needs_review |
| Bà R?a - Vtng Tàu | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Bà Rịa - Vũng Tàu | ? | needs_review |
| Bình Duong | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Bình Dương | ? | needs_review |
| Bình Phu?c | 13,14,15,16,17,18,22,23,24 | province | Bình Phước | ? | needs_review |
| Bình Thu?n | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Bình Thuận | ? | needs_review |
| Bình Ð?nh | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Bình Định | ? | needs_review |
| C? NU?C | 13,14,15,16,17,18,19,20,21,22,23,24 | national | Việt Nam | 704 (UN M49) | verified country identity |
| C?n Tho | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Cần Thơ | ? | needs_review |
| Cao B?ng | 13,14,15,16,17,18,22,23,24 | province | Cao Bằng | ? | needs_review |
| Cà Mau | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Cà Mau | ? | needs_review |
| Gia Lai | 13,14,15,16,17,18,22,23,24 | province | Gia Lai | ? | needs_review |
| H?i Duong | 13,14,15,16,17,18,22,23,24 | province | Hải Dương | ? | needs_review |
| H?i Phòng | 13,14,15,16,17,18,22,23,24 | province | Hải Phòng | ? | needs_review |
| H?u Giang | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Hậu Giang | ? | needs_review |
| Hoà Bình | 13,14,15,16,17,18,22,23,24 | province | Hòa Bình | ? | needs_review |
| Hung Yên | 13,14,15,16,17,18,22,23,24 | province | Hưng Yên | ? | needs_review |
| Hà Giang | 13,14,15,16,17,18,22,23,24 | province | Hà Giang | ? | needs_review |
| Hà N?i | 13,14,15,16,17,18,22,23,24 | province | Hà Nội | ? | needs_review |
| Hà Nam | 13,14,15,16,17,18,22,23,24 | province | Hà Nam | ? | needs_review |
| Hà Tinh | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Hà Tĩnh | ? | needs_review |
| Hà Tây | 13,14,15,16,17,18,22,23,24 | province | Hà Tây | ? | needs_review |
| Khánh Hoà | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Khánh Hòa | ? | needs_review |
| Kiên Giang | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Kiên Giang | ? | needs_review |
| Kon Tum | 13,14,15,16,17,18,22,23,24 | province | Kon Tum | ? | needs_review |
| L?ng Son | 13,14,15,16,17,18,22,23,24 | province | Lạng Sơn | ? | needs_review |
| Lai Châu | 13,14,15,16,17,18,22,23,24 | province | Lai Châu | ? | needs_review |
| Long An | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Long An | ? | needs_review |
| Lào Cai | 13,14,15,16,17,18,22,23,24 | province | Lào Cai | ? | needs_review |
| Lâm Ð?ng | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Lâm Đồng | ? | needs_review |
| Nam Ð?nh | 13,14,15,16,17,18,22,23,24 | province | Nam Định | ? | needs_review |
| Ngh? An | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Nghệ An | ? | needs_review |
| Ninh Bình | 13,14,15,16,17,18,22,23,24 | province | Ninh Bình | ? | needs_review |
| Ninh Thu?n | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Ninh Thuận | ? | needs_review |
| Phú Th? | 13,14,15,16,17,18,22,23,24 | province | Phú Thọ | ? | needs_review |
| Phú Yên | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Phú Yên | ? | needs_review |
| Qu?ng Bình | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Quảng Bình | ? | needs_review |
| Qu?ng Nam | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Quảng Nam | ? | needs_review |
| Qu?ng Ngãi | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Quảng Ngãi | ? | needs_review |
| Qu?ng Ninh | 13,14,15,16,17,18,22,23,24 | province | Quảng Ninh | ? | needs_review |
| Qu?ng Tr? | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Quảng Trị | ? | needs_review |
| Son La | 13,14,15,16,17,18,22,23,24 | province | Sơn La | ? | needs_review |
| Sóc Trang | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Sóc Trăng | ? | needs_review |
| TP.H? Chí Minh | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Thành phố Hồ Chí Minh | ? | needs_review |
| Th?a Thiên Hu? | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Thừa Thiên Huế | ? | needs_review |
| Thanh Hoá | 13,14,15,16,17,18,22,23,24 | province | Thanh Hóa | ? | needs_review |
| Thái Bình | 13,14,15,16,17,18,22,23,24 | province | Thái Bình | ? | needs_review |
| Thái Nguyên | 13,14,15,16,17,18,22,23,24 | province | Thái Nguyên | ? | needs_review |
| Ti?n Giang | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Tiền Giang | ? | needs_review |
| Trung du và mi?n núi phía B?c | 13,14,15,16,17,18,22,23,24 | region | Trung du và miền núi phía Bắc | ? | needs_review |
| Trà Vinh | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Trà Vinh | ? | needs_review |
| Tuyên Quang | 13,14,15,16,17,18,22,23,24 | province | Tuyên Quang | ? | needs_review |
| Tây Nguyên | 13,14,15,16,17,18,19,20,21,22,23,24 | region | Tây Nguyên | ? | needs_review |
| Tây Ninh | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Tây Ninh | ? | needs_review |
| Vinh Long | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Vĩnh Long | ? | needs_review |
| Vinh Phúc | 13,14,15,16,17,18,22,23,24 | province | Vĩnh Phúc | ? | needs_review |
| Yên Bái | 13,14,15,16,17,18,22,23,24 | province | Yên Bái | ? | needs_review |
| Ð?k L?k | 13,14,15,16,17,18,22,23,24 | province | Đắk Lắk | ? | needs_review |
| Ð?k Nông | 13,14,15,16,17,18,22,23,24 | province | Đắk Nông | ? | needs_review |
| Ð?ng Nai | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Đồng Nai | ? | needs_review |
| Ð?ng Tháp | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Đồng Tháp | ? | needs_review |
| Ð?ng b?ng sông C?u Long | 13,14,15,16,17,18,19,20,21,22,23,24 | region | Đồng bằng sông Cửu Long | ? | needs_review |
| Ð?ng b?ng sông H?ng | 13,14,15,16,17,18,22,23,24 | region | Đồng bằng sông Hồng | ? | needs_review |
| Ði?n Biên | 13,14,15,16,17,18,22,23,24 | province | Điện Biên | ? | needs_review |
| Ðà N?ng | 13,14,15,16,17,18,19,20,21,22,23,24 | province | Đà Nẵng | ? | needs_review |
| Ðông Nam B? | 13,14,15,16,17,18,19,20,21,22,23,24 | region | Đông Nam Bộ | ? | needs_review |
