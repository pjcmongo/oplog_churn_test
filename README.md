# oplog_churn_test
Node.js MongoDB OpLog volume/churn test

Setup: 
  - npm install
  - create data folders: mkdir test1 test2 test3 test4
  - add test files to folder test1: userWithJourney.json (user doc with journey), userWithoutJourney.json (user doc without journey)
  - add test file to folder test2: userWithJourney.json (user doc with journey)
  - add test file to folder test3: userWithJourney.json (user doc with journey)
  - add test file to folder test4: userWithJourney.json (user doc with journey)
  - edit oplog_volume_test.js to include uri to MongoDB cluster
  - optionally edit oplog_volume_test.js to set batchSize and numBatches
