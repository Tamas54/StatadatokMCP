"""A LEGÚJABB IDŐSZAK NYER, NEM AZ ELSŐ FRISS (2026-10-05).

    .venv/bin/python -m pytest -q test_lanc_legujabb.py

Élesben: HU cpi-re az `eurostat_press` júliust (1,6%) adott a 85 napos ablakon
belül, a lánc megállt, holott a `ksh_flash` augusztust (1,3%) hozta.
"""
import asyncio
import json
from datetime import datetime, timedelta

import server


def _futtat(monkeypatch, lanc, valaszok, hivasok):
    def gyar(nev):
        async def fn(spec):
            hivasok.append(nev)
            return valaszok[nev]
        return fn
    monkeypatch.setitem(server.INDICATOR_RESOLVERS, ("XX", "cpi"), [{"type": t} for t in lanc])
    for t in lanc:
        monkeypatch.setitem(server._RESOLVERS, t, gyar(t))
    f = getattr(server.get_macro_indicator, "fn", server.get_macro_indicator)
    return json.loads(asyncio.run(f("XX", "cpi")))


def _ho(n):
    d = datetime.now().replace(day=1)
    for _ in range(n):
        d = (d - timedelta(days=1)).replace(day=1)
    return d.strftime("%Y-%m")


def test_ujabb_idoszak_nyer_a_lanc_masodik_forrasabol(monkeypatch):
    h = []
    d = _futtat(monkeypatch, ["t_a", "t_b"],
                {"t_a": {"value": 1.6, "period": _ho(3), "source": "A"},
                 "t_b": {"value": 1.3, "period": _ho(2), "source": "B"}}, h)
    assert (d["value"], d["period"], d["source_used"]) == (1.3, _ho(2), "B"), d
    assert d["status"] == "fresh" and len(d["all_attempts"]) == 2


def test_egyezo_idoszaknal_a_lanc_sorrendje(monkeypatch):
    h = []
    d = _futtat(monkeypatch, ["t_a", "t_b"],
                {"t_a": {"value": 2.0, "period": _ho(2), "source": "A"},
                 "t_b": {"value": 2.1, "period": _ho(2), "source": "B"}}, h)
    assert d["source_used"] == "A"


def test_a_leheto_legfrissebbnel_nincs_elorenezes_es_webkereses_sosem(monkeypatch):
    h = []
    d = _futtat(monkeypatch, ["t_a", "t_b"],
                {"t_a": {"value": 5.5, "period": datetime.now().strftime("%Y-%m-%d"), "source": "A"},
                 "t_b": {"value": 9.9, "period": datetime.now().strftime("%Y-%m-%d"), "source": "B"}}, h)
    assert d["source_used"] == "A" and h == ["t_a"], "napi friss adat után nincs további hívás"
    h2 = []
    d2 = _futtat(monkeypatch, ["t_a", "brave_search"],
                 {"t_a": {"value": 1.6, "period": _ho(3), "source": "A"},
                  "brave_search": {"value": 1.0, "period": _ho(1), "source": "web"}}, h2)
    assert d2["source_used"] == "A" and h2 == ["t_a"], "hivatalos friss után webkeresés nem írhatja felül"
