/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

function simpleMerge(key, sourceDoc, sourceCas, sourceId, targetDoc, targetCas, targetId)
{ doc1Js = JSON.parse(sourceDoc);
  doc2Js = JSON.parse(targetDoc);
  docJs = {};
  if (sourceCas > targetCas) docJs = {...doc2Js, ...doc1Js};
  else docJs = {...doc1Js, ...doc2Js};
  let doc = JSON.stringify(docJs);
  return doc;
}
