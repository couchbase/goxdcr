/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

function slowFunc(key, sourceDoc, sourceCas, sourceId, targetDoc, targetCas, targetId) {
    sleep(4000)
    doc1Js =  JSON.parse(sourceDoc)
    doc2Js = JSON.parse(targetDoc);
    docJs = {};
    if (sourceCas > targetCas) docJs = {...doc2Js, ...doc1Js};
    else docJs = {...doc1Js, ...doc2Js};
    let doc = JSON.stringify(docJs);
    return doc;
}
function sleep(milliseconds) {
    const date = Date.now();
    let currentDate = null;
    do {
        currentDate = Date.now();
    } while (currentDate - date < milliseconds);
}