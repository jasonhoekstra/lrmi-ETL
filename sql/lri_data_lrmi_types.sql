-- Insert into AGERANGES table, maps to LRMI typicalAgeRange

INSERT INTO AGERANGES (ID, NAME) VALUES (1,'0-2');
INSERT INTO AGERANGES (ID, NAME) VALUES (2,'3-5');
INSERT INTO AGERANGES (ID, NAME) VALUES (3,'5-8');
INSERT INTO AGERANGES (ID, NAME) VALUES (4,'8-10');
INSERT INTO AGERANGES (ID, NAME) VALUES (5,'10-12');
INSERT INTO AGERANGES (ID, NAME) VALUES (6,'12-14');
INSERT INTO AGERANGES (ID, NAME) VALUES (7,'14-16');
INSERT INTO AGERANGES (ID, NAME) VALUES (8,'16-18');
INSERT INTO AGERANGES (ID, NAME) VALUES (9,'18+');

-- Insert into ALIGNMENT_TYPES table, maps to LRMI AlignmentObject

INSERT INTO ALIGNMENT_TYPES VALUES ('1', 'Assesses'), 
	('2', 'Teaches'), 
	('3', 'Requires'), 
	('4', 'TextComplexity'),
	('5', 'ReadingLevel'),
	('6', 'EducationalSubject'), 
	('7', 'EducationLevel');

-- Insert into AUDIENCES table, maps to LRMI educationalRole

INSERT INTO AUDIENCES (ID, NAME) VALUES (1,'Administrator');
INSERT INTO AUDIENCES (ID, NAME) VALUES (2,'Mentor');
INSERT INTO AUDIENCES (ID, NAME) VALUES (3,'Parent');
INSERT INTO AUDIENCES (ID, NAME) VALUES (4,'Peer Tutor');
INSERT INTO AUDIENCES (ID, NAME) VALUES (5,'Specialist');
INSERT INTO AUDIENCES (ID, NAME) VALUES (6,'Student');
INSERT INTO AUDIENCES (ID, NAME) VALUES (7,'Teacher');
INSERT INTO AUDIENCES (ID, NAME) VALUES (8,'Team');

-- Insert into INTERACTIVITY table, maps to LRMI interactivityType

INSERT INTO INTERACTIVITY (ID, NAME) VALUES (1, 'Active');
INSERT INTO INTERACTIVITY (ID, NAME) VALUES (2, 'Expositive');
INSERT INTO INTERACTIVITY (ID, NAME) VALUES (3, 'Mixed');

-- INSERT into LANGUAGES of ISO 639-2 language codes, maps to LRMI inLanguage property (184 rows/languages)

INSERT INTO LANGUAGES (ID, NAME, CODE) VALUES ('1', 'Afar', 'aa'), ('2', 'Abkhazian', 'ab'), ('3', 'Avestan', 'ae'), ('4', 'Afrikaans', 'af'), ('5', 'Akan', 'ak'), ('6', 'Amharic', 'am'), ('7', 'Aragonese', 'an'), ('8', 'Arabic', 'ar'), ('9', 'Assamese', 'as'), ('10', 'Avaric', 'av'), ('11', 'Aymara', 'ay'), ('12', 'Azerbaijani', 'az'), ('13', 'Bashkir', 'ba'), ('14', 'Belarusian', 'be'), ('15', 'Bulgarian', 'bg'), ('16', 'Bihari languages', 'bh'), ('17', 'Bislama', 'bi'), ('18', 'Bambara', 'bm'), ('19', 'Bengali', 'bn'), ('20', 'Tibetan', 'bo'), ('21', 'Breton', 'br'), ('22', 'Bosnian', 'bs'), ('23', 'Catalan; Valencian', 'ca'), ('24', 'Chechen', 'ce'), ('25', 'Chamorro', 'ch'), ('26', 'Corsican', 'co'), ('27', 'Cree', 'cr'), ('28', 'Czech', 'cs'), ('29', 'Church Slavic', 'cu'), ('30', 'Chuvash', 'cv'), ('31', 'Welsh', 'cy'), ('32', 'Danish', 'da'), ('33', 'German', 'de'), ('34', 'Divehi; Dhivehi; Maldivian', 'dv'), ('35', 'Dzongkha', 'dz'), ('36', 'Ewe', 'ee'), ('37', 'Greek', 'el'), ('38', 'English', 'en'), ('39', 'Esperanto', 'eo'), ('40', 'Spanish; Castilian', 'es'), ('41', 'Estonian', 'et'), ('42', 'Basque', 'eu'), ('43', 'Persian', 'fa'), ('44', 'Fulah', 'ff'), ('45', 'Finnish', 'fi'), ('46', 'Fijian', 'fj'), ('47', 'Faroese', 'fo'), ('48', 'French', 'fr'), ('49', 'Western Frisian', 'fy'), ('50', 'Irish', 'ga'), ('51', 'Gaelic; Scottish Gaelic', 'gd'), ('52', 'Galician', 'gl'), ('53', 'Guarani', 'gn'), ('54', 'Gujarati', 'gu'), ('55', 'Manx', 'gv'), ('56', 'Hausa', 'ha'), ('57', 'Hebrew', 'he'), ('58', 'Hindi', 'hi'), ('59', 'Hiri Motu', 'ho'), ('60', 'Croatian', 'hr'), ('61', 'Haitian; Haitian Creole', 'ht'), ('62', 'Hungarian', 'hu'), ('63', 'Armenian', 'hy'), ('64', 'Herero', 'hz'), ('65', 'Interlingua (International Auxiliary Language Association)', 'ia'), ('66', 'Indonesian', 'id'), ('67', 'Interlingue; Occidental', 'ie'), ('68', 'Igbo', 'ig'), ('69', 'Sichuan Yi; Nuosu', 'ii'), ('70', 'Inupiaq', 'ik'), ('71', 'Ido', 'io'), ('72', 'Icelandic', 'is'), ('73', 'Italian', 'it'), ('74', 'Inuktitut', 'iu'), ('75', 'Japanese', 'ja'), ('76', 'Javanese', 'jv'), ('77', 'Georgian', 'ka'), ('78', 'Kongo', 'kg'), ('79', 'Kikuyu; Gikuyu', 'ki'), ('80', 'Kuanyama; Kwanyama', 'kj'), ('81', 'Kazakh', 'kk'), ('82', 'Kalaallisut; Greenlandic', 'kl'), ('83', 'Central Khmer', 'km'), ('84', 'Kannada', 'kn'), ('85', 'Korean', 'ko'), ('86', 'Kanuri', 'kr'), ('87', 'Kashmiri', 'ks'), ('88', 'Kurdish', 'ku'), ('89', 'Komi', 'kv'), ('90', 'Cornish', 'kw'), ('91', 'Kirghiz; Kyrgyz', 'ky'), ('92', 'Latin', 'la'), ('93', 'Luxembourgish; Letzeburgesch', 'lb'), ('94', 'Ganda', 'lg'), ('95', 'Limburgan; Limburger; Limburgish', 'li'), ('96', 'Lingala', 'ln'), ('97', 'Lao', 'lo'), ('98', 'Lithuanian', 'lt'), ('99', 'Luba-Katanga', 'lu'), ('100', 'Latvian', 'lv'), ('101', 'Malagasy', 'mg'), ('102', 'Marshallese', 'mh'), ('103', 'Maori', 'mi'), ('104', 'Macedonian', 'mk'), ('105', 'Malayalam', 'ml'), ('106', 'Mongolian', 'mn'), ('107', 'Marathi', 'mr'), ('108', 'Malay', 'ms'), ('109', 'Maltese', 'mt'), ('110', 'Burmese', 'my'), ('111', 'Nauru', 'na'), ('112', 'Norwegian', 'nb'), ('113', 'Ndebele, North; North Ndebele', 'nd'), ('114', 'Nepali', 'ne'), ('115', 'Ndonga', 'ng'), ('116', 'Dutch; Flemish', 'nl'), ('117', 'Norwegian Nynorsk; Nynorsk, Norwegian', 'nn'), ('118', 'Norwegian', 'no'), ('119', 'Ndebele, South; South Ndebele', 'nr'), ('120', 'Navajo; Navaho', 'nv'), ('121', 'Chichewa; Chewa; Nyanja', 'ny'), ('122', 'Occitan (post 1500)', 'oc'), ('123', 'Ojibwa', 'oj'), ('124', 'Oromo', 'om'), ('125', 'Oriya', 'or'), ('126', 'Ossetian; Ossetic', 'os'), ('127', 'Panjabi; Punjabi', 'pa'), ('128', 'Pali', 'pi'), ('129', 'Polish', 'pl'), ('130', 'Pushto; Pashto', 'ps'), ('131', 'Portuguese', 'pt'), ('132', 'Quechua', 'qu'), ('133', 'Romansh', 'rm'), ('134', 'Rundi', 'rn'), ('135', 'Romanian; Moldavian; Moldovan', 'ro'), ('136', 'Russian', 'ru'), ('137', 'Kinyarwanda', 'rw'), ('138', 'Sanskrit', 'sa'), ('139', 'Sardinian', 'sc'), ('140', 'Sindhi', 'sd'), ('141', 'Northern Sami', 'se'), ('142', 'Sango', 'sg'), ('143', 'Sinhala; Sinhalese', 'si'), ('144', 'Slovak', 'sk'), ('145', 'Slovenian', 'sl'), ('146', 'Samoan', 'sm'), ('147', 'Shona', 'sn'), ('148', 'Somali', 'so'), ('149', 'Albanian', 'sq'), ('150', 'Serbian', 'sr'), ('151', 'Swati', 'ss'), ('152', 'Sotho, Southern', 'st'), ('153', 'Sundanese', 'su'), ('154', 'Swedish', 'sv'), ('155', 'Swahili', 'sw'), ('156', 'Tamil', 'ta'), ('157', 'Telugu', 'te'), ('158', 'Tajik', 'tg'), ('159', 'Thai', 'th'), ('160', 'Tigrinya', 'ti'), ('161', 'Turkmen', 'tk'), ('162', 'Tagalog', 'tl'), ('163', 'Tswana', 'tn'), ('164', 'Tonga (Tonga Islands)', 'to'), ('165', 'Turkish', 'tr'), ('166', 'Tsonga', 'ts'), ('167', 'Tatar', 'tt'), ('168', 'Twi', 'tw'), ('169', 'Tahitian', 'ty'), ('170', 'Uighur; Uyghur', 'ug'), ('171', 'Ukrainian', 'uk'), ('172', 'Urdu', 'ur'), ('173', 'Uzbek', 'uz'), ('174', 'Venda', 've'), ('175', 'Vietnamese', 'vi'), ('176', 'Volap', 'vo'), ('177', 'Walloon', 'wa'), ('178', 'Wolof', 'wo'), ('179', 'Xhosa', 'xh'), ('180', 'Yiddish', 'yi'), ('181', 'Yoruba', 'yo'), ('182', 'Zhuang; Chuang', 'za'), ('183', 'Chinese', 'zh'), ('184', 'Zulu', 'zu');

-- INSERT into LEARNING_RESOURCES, maps to LRMI learningResourceType property

INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (1,'Audio CD');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (2,'Audiotape');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (3,'Calculator');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (4,'CD-I');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (5,'CD-ROM');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (6,'Diskette');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (7,'Duplication Master');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (8,'DVD/Blu-ray');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (9,'E-Mail');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (10,'Electronic Slides');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (11,'Field Trip');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (12,'Filmstrip');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (13,'Flash');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (14,'Image');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (15,'In-person/Speaker');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (16,'Interactive Whiteboard');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (17,'Manipulative');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (18,'MBL (Microcomputer Based)');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (19,'Microfiche');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (20,'Overhead');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (21,'Pamphlet');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (22,'PDF');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (23,'Person-to-Person');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (24,'Phonograph Record');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (25,'Photo');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (26,'Podcast');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (27,'Printed');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (28,'Radio');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (29,'Robotics');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (30,'Satellite');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (31,'Slides');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (32,'Television');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (33,'Transparency');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (34,'Video Conference');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (35,'Videodisc');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (36,'Webpage');
INSERT INTO LEARNING_RESOURCES (ID, NAME) VALUES (37,'Wiki');


-- INSERT into PARTY_TYPES table, maps to LRMI properties of the value names

INSERT INTO PARTY_TYPES (ID, NAME) VALUES (1, 'Author');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (2, 'Publisher');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (3, 'AccountablePerson');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (4, 'Contributor');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (5, 'CopyrightHolder');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (6, 'Editor');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (7, 'SourceOrganization');
INSERT INTO PARTY_TYPES (ID, NAME) VALUES (8, 'Provider');

-- Inserts for USES table, maps to LRMI educationalUse  

INSERT INTO USES (ID, NAME) VALUES (1,'Activity');
INSERT INTO USES (ID, NAME) VALUES (2,'Analogies');
INSERT INTO USES (ID, NAME) VALUES (3,'Assessment');
INSERT INTO USES (ID, NAME) VALUES (4,'Auditory');
INSERT INTO USES (ID, NAME) VALUES (5,'Brainstorming');
INSERT INTO USES (ID, NAME) VALUES (6,'Classifying');
INSERT INTO USES (ID, NAME) VALUES (7,'Comparing');
INSERT INTO USES (ID, NAME) VALUES (8,'Cooperative Learning');
INSERT INTO USES (ID, NAME) VALUES (9,'Creative Response');
INSERT INTO USES (ID, NAME) VALUES (10,'Demonstration');
INSERT INTO USES (ID, NAME) VALUES (11,'Differentiation');
INSERT INTO USES (ID, NAME) VALUES (12,'Discovery Learning');
INSERT INTO USES (ID, NAME) VALUES (13,'Discussion/Debate');
INSERT INTO USES (ID, NAME) VALUES (14,'Drill & Practice');
INSERT INTO USES (ID, NAME) VALUES (15,'Experiential');
INSERT INTO USES (ID, NAME) VALUES (16,'Field Trip');
INSERT INTO USES (ID, NAME) VALUES (17,'Game');
INSERT INTO USES (ID, NAME) VALUES (18,'Generating Hypotheses');
INSERT INTO USES (ID, NAME) VALUES (19,'Guided Questions');
INSERT INTO USES (ID, NAME) VALUES (20,'Hands-on');
INSERT INTO USES (ID, NAME) VALUES (21,'Homework');
INSERT INTO USES (ID, NAME) VALUES (22,'Identify Similarities & Differences');
INSERT INTO USES (ID, NAME) VALUES (23,'Inquiry');
INSERT INTO USES (ID, NAME) VALUES (24,'Interactive');
INSERT INTO USES (ID, NAME) VALUES (25,'Interview/Survey');
INSERT INTO USES (ID, NAME) VALUES (26,'Interviews');
INSERT INTO USES (ID, NAME) VALUES (27,'Introduction');
INSERT INTO USES (ID, NAME) VALUES (28,'Journaling');
INSERT INTO USES (ID, NAME) VALUES (29,'Kinesthetic');
INSERT INTO USES (ID, NAME) VALUES (30,'Laboratory');
INSERT INTO USES (ID, NAME) VALUES (31,'Lecture');
INSERT INTO USES (ID, NAME) VALUES (32,'Metaphors');
INSERT INTO USES (ID, NAME) VALUES (33,'Model & Simulation');
INSERT INTO USES (ID, NAME) VALUES (34,'Musical');
INSERT INTO USES (ID, NAME) VALUES (35,'Nonlinguistic');
INSERT INTO USES (ID, NAME) VALUES (36,'Note Taking');
INSERT INTO USES (ID, NAME) VALUES (37,'Peer Coaching');
INSERT INTO USES (ID, NAME) VALUES (38,'Peer Response');
INSERT INTO USES (ID, NAME) VALUES (39,'Play');
INSERT INTO USES (ID, NAME) VALUES (40,'Presentation');
INSERT INTO USES (ID, NAME) VALUES (41,'Problem Solving');
INSERT INTO USES (ID, NAME) VALUES (42,'Problem Based');
INSERT INTO USES (ID, NAME) VALUES (43,'Project');
INSERT INTO USES (ID, NAME) VALUES (44,'Questioning');
INSERT INTO USES (ID, NAME) VALUES (45,'Reading');
INSERT INTO USES (ID, NAME) VALUES (46,'Reciprocal Teaching');
INSERT INTO USES (ID, NAME) VALUES (47,'Reflection');
INSERT INTO USES (ID, NAME) VALUES (48,'Reinforcement');
INSERT INTO USES (ID, NAME) VALUES (49,'Research');
INSERT INTO USES (ID, NAME) VALUES (50,'Review');
INSERT INTO USES (ID, NAME) VALUES (51,'Role Playing');
INSERT INTO USES (ID, NAME) VALUES (52,'Service Learning');
INSERT INTO USES (ID, NAME) VALUES (53,'Simulations');
INSERT INTO USES (ID, NAME) VALUES (54,'Summarizing');
INSERT INTO USES (ID, NAME) VALUES (55,'Technology');
INSERT INTO USES (ID, NAME) VALUES (56,'Testing Hypotheses');
INSERT INTO USES (ID, NAME) VALUES (57,'Thematic Instruction');
INSERT INTO USES (ID, NAME) VALUES (58,'Visual/Spatial');
INSERT INTO USES (ID, NAME) VALUES (59,'Word Association');
INSERT INTO USES (ID, NAME) VALUES (60,'Writing');





