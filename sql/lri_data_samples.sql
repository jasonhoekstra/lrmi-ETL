-- Sample data based on:  https://node01.public.learningregistry.net/harvest/getrecord?request_ID=1593bae53b5c4dbc8c70a9ddf8810d79&by_doc_ID=true

INSERT INTO `RESOURCES` VALUES ('1', '1593bae53b5c4dbc8c70a9ddf8810d79', 'Einstein Papers Project', 'http://www.einstein.caltech.edu/', 'provides information about the Einstein Papers Project, which publishes The Collected Papers of Albert Einstein, an edition of 25 planned volumes of Einstein\'s scientific, professional and personal papers, manuscripts and correspondence. The website lists', '1998', 'http://www.learningregistry.org/tos/cc0/v0-5/', null, 'P30M', '38', '1', '36', '1998-01-01 00:00:00', null, null, null);

INSERT INTO `PARTIES` VALUES ('1', 'California Institute of Technology, supported by National Endowment for the Humanities', 'http://caltech.edu/');

INSERT INTO `RESOURCE_AGERANGES` VALUES ('1', '1', '6'), ('2', '1', '7');

INSERT INTO `RESOURCE_AUDIENCES` VALUES ('1', '1', '6');

INSERT INTO `RESOURCE_PARTIES` VALUES ('1', '1', '1', '2');

INSERT INTO `RESOURCE_TAGS` VALUES ('1', '1', '1'), ('2', '1', '2'), ('3', '1', '3'), ('4', '1', '4'), ('5', '1', '5'), ('6', '1', '6'), ('7', '1', '7');

INSERT INTO `RESOURCE_USES` VALUES ('1', '1', '56'), ('2', '1', '60');

INSERT INTO `ALIGNMENTS` VALUES ('1', '963', '1', '2'), ('2', '964', '1', '2'), ('3', '965', '1', '2');
