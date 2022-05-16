PREFIX :     <http://example/>
PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>

DELETEWHERE { ?s ?p ?o }
;
INSERT { :s2 :p ?x } WHERE { BIND(now() AS ?x) }
