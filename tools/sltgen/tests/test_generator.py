from pathlib import Path

from generator import SltGenerator

_root_dir: Path = Path(__file__).parent.joinpath("../../../").resolve()


def test_validate_slt_script_valid():
    valid_script = """
    statement ok
    CREATE TABLE t(a Integer primary key);
    """
    slt_binary_path: Path = _root_dir.joinpath("target/debug/slt").resolve()
    generator = SltGenerator("", slt_binary_path)
    assert generator.validate_slt_script(valid_script) is None


def test_validate_slt_script_invalid():
    invalid_script = """
    # invalid data type int.
    statement ok
    CREATE TABLE t(a int);
    """

    slt_binary_path: Path = _root_dir.joinpath("target/debug/slt").resolve()
    generator = SltGenerator("", slt_binary_path)
    error = generator.validate_slt_script(invalid_script)
    assert error is not None
    assert "error" in error.lower() or "invalid" in error.lower()


def test_validate_slt_from_file():
    slt_binary_path: Path = _root_dir.joinpath("target/debug/slt").resolve()
    script_path = _root_dir.joinpath("tools/sltgen/gen/answer_1.slt").resolve()
    with open(script_path, "r") as f:
        script = f.read()
    generator = SltGenerator("", slt_binary_path)
    assert generator.validate_slt_script(script) is None
