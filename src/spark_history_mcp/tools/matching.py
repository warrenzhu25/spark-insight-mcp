"""
Stage matching and scoring utilities.

Provide consistent, configurable logic for pairing stages between applications
based on name similarity and optional time overlap.
"""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import List, Optional

from ..models.spark_types import StageData

from .common import get_config


def name_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _time_overlap_seconds(s1: StageData, s2: StageData) -> float:
    if not s1.submission_time or not s2.submission_time:
        return 0.0
    end1 = s1.completion_time or s1.submission_time
    end2 = s2.completion_time or s2.submission_time
    latest_start = max(s1.submission_time, s2.submission_time)
    earliest_end = min(end1, end2)
    delta = (earliest_end - latest_start).total_seconds()
    return max(0.0, delta)


@dataclass
class StageMatch:
    stage1: StageData
    stage2: StageData
    similarity: float
    overlap_seconds: float


def match_stages(
    stages1: List[StageData],
    stages2: List[StageData],
    similarity_threshold: Optional[float] = None,
    require_overlap: bool = False,
) -> List[StageMatch]:
    """Greedy one-to-one matching using name similarity and optional overlap.

    Returns a list of StageMatch with scores, sorted by similarity desc.
    """
    cfg = get_config()
    sim_thresh = (
        similarity_threshold if similarity_threshold is not None else cfg.stage_match_similarity
    )

    matches: List[StageMatch] = []
    used2: set[int] = set()

    for s1 in stages1:
        best_idx = -1
        best_sim = 0.0
        best_overlap = 0.0
        for i, s2 in enumerate(stages2):
            if i in used2:
                continue
            sim = name_similarity(s1.name, s2.name)
            if sim < sim_thresh:
                continue
            overlap = _time_overlap_seconds(s1, s2)
            if require_overlap and overlap <= 0:
                continue
            if sim > best_sim or (sim == best_sim and overlap > best_overlap):
                best_sim = sim
                best_idx = i
                best_overlap = overlap
        if best_idx >= 0:
            matches.append(StageMatch(s1, stages2[best_idx], best_sim, best_overlap))
            used2.add(best_idx)

    # Highest similarity first
    matches.sort(key=lambda m: (m.similarity, m.overlap_seconds), reverse=True)
    return matches

