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