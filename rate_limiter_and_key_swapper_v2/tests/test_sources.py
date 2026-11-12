import pytest

from fakes import FakeProvider, build, run
from keyswap import keys_from_env, keys_from_file, parse_dotenv, read_dotenv


def test_dotenv_parser_handles_real_world_files():
    text = """
    # comment line
    API_KEYS=k1,k2
    export TOKEN="abc def"
    SINGLE='quoted#hash'
    INLINE=value   # trailing comment
    EMPTY=
    no equals sign here
    =novalue
    """
    env = parse_dotenv(text)
    assert env["API_KEYS"] == "k1,k2"
    assert env["TOKEN"] == "abc def"
    assert env["SINGLE"] == "quoted#hash"       # # inside quotes is kept
    assert env["INLINE"] == "value"             # # after whitespace is a comment
    assert env["EMPTY"] == ""
    assert "no equals sign here" not in env and "" not in env


def test_missing_dotenv_is_just_empty(tmp_path):
    assert read_dotenv(str(tmp_path / "nope.env")) == {}


def test_keys_from_env_comma_list_and_numbered_styles():
    env = {"API_KEYS": "a, b,c", "API_KEYS_2": "e", "API_KEYS_1": "d", "OTHER": "zzz", "API_KEYS_X": "no"}
    assert keys_from_env("API_KEYS", environ=env) == ["a", "b", "c", "d", "e"]  # numbered sorted by number


def test_real_env_beats_dotenv_and_both_work(tmp_path):
    f = tmp_path / ".env"
    f.write_text("GEMINI_KEYS=from-file-1,from-file-2\nOTHER=1\n")
    assert keys_from_env("GEMINI_KEYS", dotenv_path=str(f), environ={}) == ["from-file-1", "from-file-2"]
    assert keys_from_env("GEMINI_KEYS", dotenv_path=str(f), environ={"GEMINI_KEYS": "real"}) == ["real"]


def test_dedup_keeps_order():
    assert keys_from_env("K", environ={"K": "a,b,a,c,b"}) == ["a", "b", "c"]


def test_keys_from_file(tmp_path):
    f = tmp_path / "keys.txt"
    f.write_text("# my keys\nk1\n\nk2\nk1\n")
    assert keys_from_file(str(f)) == ["k1", "k2"]


def test_gateway_loads_from_env_and_dotenv_and_file(tmp_path, monkeypatch):
    dotenv = tmp_path / ".env"
    dotenv.write_text("MY_KEYS=envkey1,envkey2\n")
    keyfile = tmp_path / "more.txt"
    keyfile.write_text("filekey1\nenvkey2\n")           # envkey2 repeats on purpose
    monkeypatch.delenv("MY_KEYS", raising=False)

    g, _ = build(FakeProvider(), keys=())
    ids = g.add_keys_from_env("MY_KEYS", dotenv=str(dotenv))
    ids += g.add_keys_from_file(str(keyfile))           # duplicate is skipped, not an error
    ids += [g.add_key("runtime-key", key_id="runtime")]  # plain runtime registration still works
    assert len(g.status()) == 4
    assert {r["key_id"] for r in g.status()} >= {"runtime"}

    reply = run(g.chat({"model": "m"}))                  # and they actually serve traffic
    assert reply.ok


def test_bad_key_in_env_is_rejected_without_echoing_it():
    g, _ = build(FakeProvider(), keys=())
    with pytest.raises(ValueError) as info:
        g.add_keys(["good", "bad key with spaces"])
    assert "bad key with spaces" not in str(info.value)
