from h2mi.scanner.classify import classify_columns, findings, loadable


def test_classes_and_loadable():
    plans = classify_columns([("id", "bigint"), ("qty", "tinyint"), ("u", "uniontype<int,string>")])
    classes = {f.column: f.klass for f in findings(plans)}
    assert classes == {"qty": "AUTO_MAP", "u": "REDESIGN"}
    assert not loadable(plans)
    assert loadable(plans[:2])


def test_comments_are_carried():
    plans = classify_columns([("id", "bigint")], comments={"id": "primary key"})
    assert plans[0].comment == "primary key"
