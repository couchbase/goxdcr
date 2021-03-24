function simpleMerge(key, sourceDoc, sourceCas, sourceId, targetDoc, targetCas, targetId)
{ doc1Js = JSON.parse(sourceDoc);
  doc2Js = JSON.parse(targetDoc);
  docJs = {};
  if (sourceCas > targetCas) docJs = {...doc2Js, ...doc1Js};
  else docJs = {...doc1Js, ...doc2Js};
  let doc = JSON.stringify(docJs);
  return doc;
}
