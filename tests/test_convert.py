from stages.convert import valid_cas,canonical_cas,concepts
def test_n_hexane_cas():assert valid_cas("110-54-3") and not valid_cas("110-54-4")
def test_unhyphenated_cas():assert canonical_cas("110543")=="110-54-3"
def test_conclusion_is_scoped():
 row={"publication_status":"Final screening assessment published","conclusion":"Does not meet section 64 of CEPA"};assert ("conclusion","Does not meet section 64 of CEPA") in concepts("assessments",row)
def test_challenge_separate():assert concepts("challenge_substances",{"batch_number":"4","batch_status":"Finalized","categorization_decision":"Health"})==[("batch_number","4"),("batch_status","Finalized"),("categorization_decision","Health")]
