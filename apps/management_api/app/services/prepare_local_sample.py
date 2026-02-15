from __future__ import annotations

import hashlib
import logging
import shutil
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_ARTIFACT_ROOT = Path("/tmp/pipelineforge_artifacts")
_DATAFINER_SAMPLE_PATH = _ARTIFACT_ROOT / "datafiner_input.lance"

# ---------------------------------------------------------------------------
# Corpus layout — each source gets its own Lance file
# ---------------------------------------------------------------------------

_CORPUS_SPECS: list[tuple[str, int]] = [
    ("dclm", 35_000),
    ("fineweb", 25_000),
    ("fineweb-edu-zh", 15_000),
    ("the-stack", 15_000),
    ("megamath", 10_000),
]

CORPUS_PATHS: dict[str, Path] = {
    name: _ARTIFACT_ROOT / f"corpus_{name.replace('-', '_')}.lance"
    for name, _ in _CORPUS_SPECS
}

_TOTAL_ROWS = sum(count for _, count in _CORPUS_SPECS)  # 100,000

# ---------------------------------------------------------------------------
# Deterministic helpers (no `random` module)
# ---------------------------------------------------------------------------

_SOURCE_DOMAINS: dict[str, list[str]] = {
    "dclm": ["stem", "humanities", "general", "social_science"],
    "fineweb": ["general", "humanities", "social_science"],
    "fineweb-edu-zh": ["stem", "humanities", "social_science"],
    "the-stack": ["code"],
    "megamath": ["math"],
}

_DOMAIN_TEMPLATES: dict[str, list[str]] = {
    "stem": [
        (
            "The experiment measured the relationship between {topic_a} and"
            " {topic_b} across multiple trials under controlled laboratory"
            " conditions with careful attention to systematic error sources"
            " and reproducibility of measurements in the {field} domain"
        ),
        (
            "Researchers observed that increasing {topic_a} levels resulted"
            " in a proportional decrease of {topic_b} concentrations when"
            " the system was held at equilibrium in the {field} research"
            " facility during the extended observation period"
        ),
        (
            "A comprehensive review of {field} literature reveals significant"
            " advances in understanding {topic_a} mechanisms and their"
            " interaction with {topic_b} pathways in biological and physical"
            " systems over the past decade of scientific inquiry"
        ),
        (
            "The computational model predicts that {topic_a} fluctuations"
            " can be minimized by adjusting {topic_b} parameters within the"
            " tolerance range specified by the {field} standards committee"
            " for calibration protocols"
        ),
    ],
    "humanities": [
        (
            "The narrative structure of the text reveals complex themes of"
            " {topic_a} and {topic_b} that resonate with broader cultural"
            " movements in {field} studies throughout the modern and"
            " postmodern literary periods"
        ),
        (
            "Historical analysis of {topic_a} demonstrates how societal"
            " attitudes toward {topic_b} shifted dramatically during this"
            " era according to multiple primary source documents in {field}"
            " archives and museum collections"
        ),
        (
            "Philosophical discourse surrounding {topic_a} has been"
            " fundamentally shaped by competing interpretations of"
            " {topic_b} in the context of {field} thought and its enduring"
            " influence on contemporary intellectual traditions"
        ),
        (
            "The artistic movement drew heavily from {topic_a} imagery and"
            " symbolic representations of {topic_b} to communicate deeper"
            " truths about the human experience as documented in {field}"
            " scholarship and critical reviews"
        ),
    ],
    "code": [
        (
            "def process_{topic_a}(data: list[dict]) -> list[dict]:\n"
            "    results = []\n"
            "    for item in data:\n"
            "        if item.get('{topic_b}') is not None:\n"
            "            transformed = {{k: v for k, v in item.items()"
            " if k != '{topic_b}'}}\n"
            "            transformed['processed'] = True\n"
            "            results.append(transformed)\n"
            "    return results"
        ),
        (
            "class {topic_a}Handler:\n"
            "    def __init__(self, config: dict):\n"
            "        self.config = config\n"
            "        self.{topic_b}_cache = {{}}\n"
            "    def execute(self, payload: dict) -> dict:\n"
            "        key = payload.get('id', '')\n"
            "        if key in self.{topic_b}_cache:\n"
            "            return self.{topic_b}_cache[key]\n"
            "        result = self._compute(payload)\n"
            "        self.{topic_b}_cache[key] = result\n"
            "        return result"
        ),
        (
            "async def fetch_{topic_a}_data(session, endpoint:"
            " str, params: dict) -> dict:\n"
            "    url = f'{{endpoint}}/{topic_b}'\n"
            "    async with session.get(url, params=params)"
            " as response:\n"
            "        if response.status != 200:\n"
            "            raise RuntimeError(f'Failed to fetch"
            " {topic_a}: {{response.status}}')\n"
            "        return await response.json()"
        ),
        (
            "import logging\n"
            "logger = logging.getLogger(__name__)\n\n"
            "def validate_{topic_a}(record: dict,"
            " schema: dict) -> bool:\n"
            "    for field, field_type in schema.items():\n"
            "        value = record.get(field)\n"
            "        if value is None:\n"
            "            logger.warning('Missing field %s in"
            " {topic_b} record', field)\n"
            "            return False\n"
            "        if not isinstance(value, field_type):\n"
            "            return False\n"
            "    return True"
        ),
    ],
    "math": [
        (
            "Let f(x) = {topic_a}(x) be a continuous function on the"
            " interval [0,1]. We prove that the integral of f converges"
            " under the {topic_b} norm when the Lipschitz constant is"
            " bounded by the {field} criterion for uniform convergence"
            " of function series"
        ),
        (
            "Theorem: For any {topic_a} matrix A of dimension n, the"
            " eigenvalues satisfy the {topic_b} bound when the spectral"
            " radius is constrained by the {field} inequality derived"
            " from the minimax characterization of singular values"
        ),
        (
            "Consider the optimization problem where we minimize the"
            " {topic_a} objective subject to {topic_b} constraints in"
            " the feasible region defined by the {field} polytope with"
            " vertices at the extreme points of the constraint set"
        ),
        (
            "The probability that a random {topic_a} variable exceeds"
            " the {topic_b} threshold is bounded above by the {field}"
            " concentration inequality which provides exponential tail"
            " decay for sub-Gaussian distributions"
        ),
    ],
    "general": [
        (
            "The rapid development of {topic_a} technology has transformed"
            " how people interact with {topic_b} systems in everyday life"
            " creating new opportunities and challenges for individuals"
            " and organizations across the {field} sector"
        ),
        (
            "Market analysts predict that {topic_a} investments will"
            " continue to grow as consumer demand for {topic_b} products"
            " and services increases across the {field} industry throughout"
            " the coming fiscal quarters and beyond"
        ),
        (
            "Community leaders emphasized the importance of {topic_a}"
            " education programs in preparing the next generation for"
            " careers in {topic_b} and related fields as the {field}"
            " economy continues its rapid transformation"
        ),
        (
            "The environmental impact assessment concluded that {topic_a}"
            " emissions could be reduced by implementing {topic_b}"
            " mitigation strategies recommended by the {field} advisory"
            " panel and supported by recent peer-reviewed research"
        ),
    ],
    "social_science": [
        (
            "Survey data collected from participants across multiple"
            " demographic groups indicates that attitudes toward {topic_a}"
            " vary significantly based on {topic_b} exposure and prior"
            " experience in the {field} domain of study"
        ),
        (
            "The longitudinal study tracked changes in {topic_a} behavior"
            " patterns over a period of several years and found strong"
            " correlations with {topic_b} policy interventions implemented"
            " at the regional level in {field} contexts"
        ),
        (
            "Cross-cultural comparison reveals that {topic_a} norms differ"
            " substantially between societies that prioritize {topic_b}"
            " values and those that emphasize alternative frameworks within"
            " the broader {field} theoretical landscape"
        ),
        (
            "Economic modeling suggests that {topic_a} incentive structures"
            " can be redesigned to promote {topic_b} outcomes when"
            " policymakers account for behavioral biases documented in"
            " the {field} experimental literature"
        ),
    ],
}

_TOPICS_A = [
    "neural", "gradient", "entropy", "spectral", "stochastic", "quantum", "thermal",
    "optical", "acoustic", "kinetic", "voltage", "magnetic", "orbital", "genomic",
    "proteomic", "cellular", "vascular", "cognitive", "linguistic", "semantic",
]
_TOPICS_B = [
    "convergence", "diffusion", "resonance", "absorption", "emission", "synthesis",
    "catalysis", "oxidation", "reduction", "polarization", "modulation", "regression",
    "classification", "clustering", "segmentation", "embedding", "inference", "sampling",
    "normalization", "calibration",
]
_FIELDS = [
    "physics", "chemistry", "biology", "mathematics", "engineering", "medicine",
    "ecology", "geology", "astronomy", "computer_science", "linguistics", "economics",
    "sociology", "psychology", "philosophy", "anthropology", "political_science",
    "education", "environmental_science", "materials_science",
]


def _deterministic_int(idx: int, salt: str) -> int:
    return int(hashlib.sha256(f"{idx}:{salt}".encode("utf-8")).hexdigest()[:8], 16)


def _pick_domain(idx: int, source: str) -> str:
    domains = _SOURCE_DOMAINS[source]
    return domains[_deterministic_int(idx, "domain") % len(domains)]


def _generate_text(idx: int, domain: str) -> str:
    templates = _DOMAIN_TEMPLATES[domain]
    template_idx = _deterministic_int(idx, "template") % len(templates)
    template = templates[template_idx]

    topic_a = _TOPICS_A[_deterministic_int(idx, "topic_a") % len(_TOPICS_A)]
    topic_b = _TOPICS_B[_deterministic_int(idx, "topic_b") % len(_TOPICS_B)]
    field = _FIELDS[_deterministic_int(idx, "field") % len(_FIELDS)]

    text = template.format(topic_a=topic_a, topic_b=topic_b, field=field)

    # Extend shorter texts by repeating with variation to reach 30-300 word range
    repeat_count = _deterministic_int(idx, "repeat") % 4
    if repeat_count > 0:
        suffixes = [
            f"Further analysis in the {field} context confirms these observations.",
            f"Additional {topic_a} measurements support the {topic_b} hypothesis.",
            f"The {field} community has noted the significance of these findings.",
            f"Subsequent {topic_a} experiments yielded consistent {topic_b} results.",
        ]
        for r in range(repeat_count):
            text += " " + suffixes[_deterministic_int(idx, f"suffix_{r}") % len(suffixes)]

    return text


def _build_corpus_rows(source: str, count: int, global_offset: int) -> list[dict[str, Any]]:
    """Generate *count* rows for a single corpus source."""
    rows: list[dict[str, Any]] = []
    language = "zh" if source == "fineweb-edu-zh" else "en"
    # ~5% near-duplicates within each corpus: first 5% of rows copy from the
    # corresponding row one corpus-length ahead in the global ID space, with a
    # small suffix so MinHash dedup has real work.
    dup_boundary = int(count * 0.05)

    for local_idx in range(count):
        idx = global_offset + local_idx  # globally unique row id
        domain = _pick_domain(idx, source)
        score = round(_deterministic_int(idx, "score") / 0xFFFFFFFF, 6)

        if local_idx < dup_boundary:
            donor_idx = global_offset + dup_boundary + local_idx
            donor_domain = _pick_domain(donor_idx, source)
            text = _generate_text(donor_idx, donor_domain) + f" [dup-{idx}]"
        else:
            text = _generate_text(idx, domain)

        rows.append(
            {
                "id": idx,
                "source_id": f"src-{idx % 3}",
                "source": source,
                "domain": domain,
                "language": language,
                "score": score,
                "text": text,
                "question": f"What is the expected behavior for sample {idx}?",
                "items": [f"item-{idx}", f"item-{idx + 1}"],
                "conversation": [
                    {"role": "user", "content": f"Hello from row {idx}."},
                    {"role": "assistant", "content": "Acknowledged and processed."},
                ],
                "category": "train" if idx % 2 == 0 else "eval",
                "url": f"https://corpus.example.com/{source}/{domain}/{idx}",
            }
        )
    return rows


def _sample_rows() -> list[dict[str, Any]]:
    """Return all 100,000 rows across every corpus (used for the combined
    backward-compatible Lance file consumed by the 10 existing templates)."""
    all_rows: list[dict[str, Any]] = []
    offset = 0
    for source, count in _CORPUS_SPECS:
        all_rows.extend(_build_corpus_rows(source, count, offset))
        offset += count
    return all_rows


def _wipe_path(path: Path) -> None:
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def prepare_local_sample(*, force: bool = False) -> Path:
    try:
        import daft  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency dependent
        raise RuntimeError("Daft with Lance support is required to prepare local sample datasets.") from exc

    _ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)

    # Fast-path: all files already exist
    all_exist = _DATAFINER_SAMPLE_PATH.exists() and all(p.exists() for p in CORPUS_PATHS.values())
    if all_exist and not force:
        logger.info("Using existing local sample datasets at %s", _ARTIFACT_ROOT)
        return _DATAFINER_SAMPLE_PATH

    # ---- Generate rows per corpus and write individual Lance files --------
    combined_rows: list[dict[str, Any]] = []
    offset = 0
    for source, count in _CORPUS_SPECS:
        corpus_rows = _build_corpus_rows(source, count, offset)
        combined_rows.extend(corpus_rows)
        offset += count

        corpus_path = CORPUS_PATHS[source]
        _wipe_path(corpus_path)
        daft.from_pylist(corpus_rows).write_lance(str(corpus_path), mode="overwrite")
        logger.info("Wrote %d rows to %s", len(corpus_rows), corpus_path)

    # ---- Write combined file (backward compat for existing templates) -----
    _wipe_path(_DATAFINER_SAMPLE_PATH)
    daft.from_pylist(combined_rows).write_lance(str(_DATAFINER_SAMPLE_PATH), mode="overwrite")
    logger.info(
        "Prepared local sample datasets (%d rows, %d corpora) at %s",
        len(combined_rows),
        len(_CORPUS_SPECS),
        _ARTIFACT_ROOT,
    )
    return _DATAFINER_SAMPLE_PATH


if __name__ == "__main__":
    prepared_path = prepare_local_sample()
    print(prepared_path)
