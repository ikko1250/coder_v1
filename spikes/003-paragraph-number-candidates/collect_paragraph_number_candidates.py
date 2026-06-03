from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Optional, Sequence


DIGIT_VALUES = {
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "7": 7,
    "8": 8,
    "9": 9,
    "０": 0,
    "１": 1,
    "２": 2,
    "３": 3,
    "４": 4,
    "５": 5,
    "６": 6,
    "７": 7,
    "８": 8,
    "９": 9,
}


OPENING_BRACKETS = {
    "(": ")",
    "（": "）",
    "「": "」",
    "『": "』",
    "【": "】",
    "［": "］",
    "〈": "〉",
    "《": "》",
}
ASCII_QUOTES = {"\"", "'"}


def mojibakeVariants(text: str) -> set[str]:
    variants = {text}
    encoded = text.encode("utf-8")
    for encoding in ("cp1252", "latin-1"):
        try:
            variants.add(encoded.decode(encoding))
        except UnicodeDecodeError:
            variants.add(encoded.decode(encoding, errors="ignore"))
    return variants


def expandVariants(values: Iterable[str]) -> list[str]:
    expanded: set[str] = set()
    for value in values:
        expanded.update(mojibakeVariants(value))
    return sorted(expanded, key=lambda item: (-len(item), item))


for fullwidthDigit, value in list(DIGIT_VALUES.items()):
    if not fullwidthDigit.isascii():
        for variant in mojibakeVariants(fullwidthDigit):
            DIGIT_VALUES.setdefault(variant, value)

DIGIT_TOKENS = sorted(DIGIT_VALUES, key=lambda item: (-len(item), item))

OPENING_BRACKET_TOKENS: dict[str, str] = {}
CLOSING_BRACKET_TOKENS: dict[str, str] = {}
for left, right in OPENING_BRACKETS.items():
    leftVariants = mojibakeVariants(left)
    rightVariants = mojibakeVariants(right)
    for leftVariant in leftVariants:
        OPENING_BRACKET_TOKENS[leftVariant] = right
    for rightVariant in rightVariants:
        CLOSING_BRACKET_TOKENS[rightVariant] = left

OPENING_BRACKET_MATCHES = sorted(OPENING_BRACKET_TOKENS, key=lambda item: (-len(item), item))
CLOSING_BRACKET_MATCHES = sorted(CLOSING_BRACKET_TOKENS, key=lambda item: (-len(item), item))

SEPARATORS = {",", "，", "/", "／", ".", "．"}

OPENER_PATTERNS = expandVariants(
    [
        "事業者は",
        "設置者は",
        "申請者は",
        "届出者は",
        "市長は",
        "町長は",
        "村長は",
        "知事は",
        "前項",
        "前二項",
        "前３項",
        "前各項",
        "前条",
        "この条例",
        "この規則",
        "条例",
        "規則",
        "法",
        "地域住民",
        "近隣関係者",
        "説明会",
        "協議",
        "許可",
        "認定",
    ]
)

PRECEDING_PATTERNS = expandVariants(
    [
        "事項",
        "区域",
        "書類",
        "図書",
        "もの",
        "場合",
        "基準",
        "とおり",
        "認める事項",
        "規則で定める事項",
        "前各号に掲げるもの",
        "前各号に定めるもの",
    ]
)

ARTICLE_PREFIXES = expandVariants(["第"])
ARTICLE_UNITS = expandVariants(["条", "項", "号", "章", "節", "款", "目"])
SUBNUMBER_PREFIXES = expandVariants(["条の", "項の", "号の"])
RELATIVE_ARTICLE_PREFIXES = expandVariants(["前", "同"])
ERA_PREFIXES = expandVariants(["令和", "平成", "昭和"])
DATE_UNITS = expandVariants(["年", "月", "日", "時"])
QUANTITY_UNITS = expandVariants(
    [
        "通",
        "部",
        "親等",
        "キロワット",
        "メガワット",
        "平方メートル",
        "メートル",
        "ｍ",
        "m",
        "㎡",
        "万円",
        "%",
        "％",
        "円",
        "人",
        "件",
        "回",
        "ヶ月",
        "か月",
        "箇月",
        "週間",
        "以上",
        "未満",
        "以下",
        "超",
    ]
)
FRACTION_PARTS = expandVariants(["分の"])
FORM_PREFIXES = expandVariants(["様式第", "別記様式第"])
FORM_SUFFIXES = expandVariants(["号", "号様式"])
FORM_REVERSED_SUFFIXES = expandVariants(["号様式"])
TABLE_PREFIXES = expandVariants(["別表第", "別記", "表"])
TABLE_SUFFIXES = expandVariants(["面"])
NOTE_PREFIXES = expandVariants(["※", "注"])
REVISION_SOURCE_TOKENS = expandVariants(["条例", "規則", "告示"])
REVISION_ACTION_TOKENS = expandVariants(["改正", "追加", "全改", "繰下", "繰上"])
REVISION_ERA_PREFIXES = expandVariants(["令", "令和", "平", "平成", "昭", "昭和"])
REVISION_PREFIXES = expandVariants(["旧第"])
OCR_INTRUSION_PREFIXES = expandVariants(["に", "又"])
OCR_INTRUSION_SUFFIXES = expandVariants(["より", "は"])
LEGAL_NEARBY_PATTERNS = expandVariants(["条例第", "規則第", "法第", "第"])
SUBJECT_PARTICLE = expandVariants(["は"])
SENTENCE_PERIODS = expandVariants(["。"])
LEGAL_ITEM_MARKERS = expandVariants(["⑴", "⑵", "⑶", "⑷", "⑸", "⑹", "⑺", "⑻", "⑼", "ア", "イ", "ウ", "エ", "オ", "カ", "キ"])


@dataclass
class NumericSpan:
    spanId: int
    start: int
    end: int
    rawText: str
    normalizedText: str
    digitCount: int
    markerValue: Optional[int]
    spanShape: str


@dataclass
class SentenceRow:
    runId: int
    sentenceId: int
    paragraphId: int
    documentId: int
    fileName: str
    sourceFilePath: str
    isTableParagraph: int
    sentenceNoInDocument: int
    sentenceNoInParagraph: int
    sentenceText: str
    paragraphText: str
    previousSentenceText: str = ""
    nextSentenceText: str = ""
    sentenceCharOffsetInParagraph: int = -1
    offsetResolutionReason: str = "not_resolved"


@dataclass
class Candidate:
    candidate_id: str
    run_id: int
    sentence_id: int
    paragraph_id: int
    document_id: int
    file_name: str
    source_file_path: str
    is_table_paragraph: int
    sentence_no_in_document: int
    sentence_no_in_paragraph: int
    marker: str
    marker_value: str
    offset: int
    candidate_type: str
    confidence: str
    actionability: str
    split_decision: str
    before_context: str
    after_context: str
    sentence_text: str
    paragraph_text_sample: str
    positive_reasons: str
    negative_reasons: str
    opener_pattern: str
    preceding_pattern: str
    following_pattern: str
    run_members: str
    broad_rule_matched: int
    narrow_opener_matched: int
    would_be_in_targeted_135: int
    targeted_exclusion_reasons: str
    collection_stage: str
    audit_group: str
    matched_negative_span_start: str
    matched_negative_span_end: str
    matched_negative_text: str
    negative_span_id: str
    marker_global_offset_in_paragraph: str
    sentence_char_offset_in_paragraph: str
    paragraph_char_offset: str
    offset_resolution_reason: str
    previous_sentence_text: str
    next_sentence_text: str
    before_context_len: int
    after_context_len: int
    numeric_span_id: int
    numeric_span_start: int
    numeric_span_end: int
    numeric_span_text: str
    numeric_span_normalized_text: str
    numeric_span_shape: str
    paragraph_full_text: str = ""


CSV_FIELDS = [
    field
    for field in Candidate.__dataclass_fields__
    if field != "paragraph_full_text"
]


def parseArgs(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect report-only Japanese legal paragraph-number candidates from an analysis DB."
    )
    parser.add_argument("--analysis-db", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--context-len", type=int, default=80)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--run-id", type=int, default=None)
    return parser.parse_args(argv)


def matchAnyAt(text: str, index: int, tokens: Sequence[str]) -> Optional[str]:
    for token in tokens:
        if token and text.startswith(token, index):
            return token
    return None


def matchDigitAt(text: str, index: int) -> Optional[tuple[str, int]]:
    token = matchAnyAt(text, index, DIGIT_TOKENS)
    if token is None:
        return None
    return token, DIGIT_VALUES[token]


def nextDigitStarts(text: str, index: int) -> bool:
    return matchDigitAt(text, index) is not None


def tokenizeNumericSpans(text: str) -> list[NumericSpan]:
    spans: list[NumericSpan] = []
    index = 0
    spanId = 1
    while index < len(text):
        digitMatch = matchDigitAt(text, index)
        if digitMatch is None:
            index += 1
            continue

        start = index
        normalizedParts: list[str] = []
        digitCount = 0
        hasSeparator = False

        while index < len(text):
            digitMatch = matchDigitAt(text, index)
            if digitMatch is not None:
                token, value = digitMatch
                normalizedParts.append(str(value))
                digitCount += 1
                index += len(token)
                continue

            char = text[index]
            if char in SEPARATORS and index + 1 < len(text) and nextDigitStarts(text, index + 1):
                normalizedParts.append(char)
                hasSeparator = True
                index += 1
                continue
            break

        rawText = text[start:index]
        normalizedText = "".join(normalizedParts)
        markerValue: Optional[int] = None
        if digitCount == 1 and not hasSeparator:
            markerValue = int(re.sub(r"\D", "", normalizedText) or "0")
            spanShape = "single_digit" if markerValue != 0 else "zero_single_digit"
        elif hasSeparator:
            spanShape = "formatted_numeric_span"
        else:
            spanShape = "multi_digit_numeric_span"
        spans.append(
            NumericSpan(
                spanId=spanId,
                start=start,
                end=index,
                rawText=rawText,
                normalizedText=normalizedText,
                digitCount=digitCount,
                markerValue=markerValue,
                spanShape=spanShape,
            )
        )
        spanId += 1
    return spans


def startsWithAny(text: str, patterns: Sequence[str]) -> str:
    for pattern in patterns:
        if pattern and text.startswith(pattern):
            return pattern
    return ""


def endsWithAny(text: str, patterns: Sequence[str], window: int = 24) -> str:
    sample = text[-window:]
    for pattern in patterns:
        if pattern and sample.endswith(pattern):
            return pattern
    return ""


def containsAny(text: str, patterns: Sequence[str]) -> str:
    for pattern in patterns:
        if pattern and pattern in text:
            return pattern
    return ""


def stripLocalSpaces(text: str) -> str:
    return text.strip(" \t　")


def localWindow(row: SentenceRow, span: NumericSpan, beforeLen: int = 16, afterLen: int = 24) -> str:
    return row.sentenceText[max(0, span.start - beforeLen) : min(len(row.sentenceText), span.end + afterLen)]


def resolveBracketQuoteDepth(text: str, targetIndex: int) -> tuple[int, int]:
    bracketStack: list[str] = []
    quoteStack: list[str] = []
    index = 0
    while index < len(text) and index < targetIndex:
        char = text[index]
        if char in ASCII_QUOTES:
            if quoteStack and quoteStack[-1] == char:
                quoteStack.pop()
            else:
                quoteStack.append(char)
            index += 1
            continue

        opening = matchAnyAt(text, index, OPENING_BRACKET_MATCHES)
        if opening is not None:
            bracketStack.append(OPENING_BRACKET_TOKENS[opening])
            index += len(opening)
            continue

        closing = matchAnyAt(text, index, CLOSING_BRACKET_MATCHES)
        if closing is not None:
            if bracketStack:
                bracketStack.pop()
            index += len(closing)
            continue

        index += 1
    return len(bracketStack), len(quoteStack)


def hasMarkdownPipeNoise(text: str) -> bool:
    return "|" in text or "||" in text


def isSentenceInitial(row: SentenceRow, span: NumericSpan) -> bool:
    return row.sentenceText[: span.start].strip() == ""


def detectNarrowOpener(afterText: str) -> str:
    direct = startsWithAny(afterText.lstrip(), OPENER_PATTERNS)
    if direct:
        return direct
    return containsAny(afterText[:18], OPENER_PATTERNS)


def detectWeakSubject(afterText: str) -> str:
    sample = afterText.lstrip()[:16]
    for particle in SUBJECT_PARTICLE:
        index = sample.find(particle)
        if 0 < index <= 10:
            return sample[: index + len(particle)]
    return ""


def detectPrecedingEvidence(beforeText: str) -> str:
    preceding = endsWithAny(beforeText.rstrip(), PRECEDING_PATTERNS)
    if preceding:
        return preceding
    if endsWithAny(beforeText.rstrip(), SENTENCE_PERIODS, window=4):
        return "preceding_sentence_period"
    sample = beforeText[-40:]
    markerCount = sum(1 for marker in LEGAL_ITEM_MARKERS if marker in sample)
    if markerCount >= 2:
        return "completed_legal_item_body"
    return ""


def detectExplicitRun(spans: Sequence[NumericSpan]) -> dict[int, list[NumericSpan]]:
    singleSpans = [
        span
        for span in spans
        if span.spanShape == "single_digit" and span.markerValue is not None and span.markerValue > 0
    ]
    runBySpan: dict[int, list[NumericSpan]] = {}
    current: list[NumericSpan] = []
    for span in singleSpans:
        if not current or span.markerValue == (current[-1].markerValue or 0) + 1:
            current.append(span)
        else:
            if len(current) >= 2:
                for member in current:
                    runBySpan[member.spanId] = current
            current = [span]
    if len(current) >= 2:
        for member in current:
            runBySpan[member.spanId] = current
    return runBySpan


def detectNegativeEvidence(row: SentenceRow, span: NumericSpan) -> tuple[str, list[str], str, str, str, str]:
    before = row.sentenceText[: span.start]
    after = row.sentenceText[span.end :]
    beforeStripped = stripLocalSpaces(before)
    afterStripped = stripLocalSpaces(after)
    windowBefore = beforeStripped[-16:]
    windowAfter = afterStripped[:16]
    localText = localWindow(row, span)
    matchedText = ""
    spanStart = ""
    spanEnd = ""
    negativeSpanId = ""

    def markerLocal(reason: str) -> tuple[str, list[str], str, str, str, str]:
        nonlocal matchedText, spanStart, spanEnd, negativeSpanId
        matchedText = row.sentenceText[max(0, span.start - 8) : min(len(row.sentenceText), span.end + 8)]
        spanStart = str(span.start)
        spanEnd = str(span.end)
        negativeSpanId = str(span.spanId)
        return reason, [reason], spanStart, spanEnd, matchedText, negativeSpanId

    if span.spanShape in {"multi_digit_numeric_span", "zero_single_digit"}:
        return markerLocal("multi_digit_numeric_span")
    if span.spanShape == "formatted_numeric_span":
        return markerLocal("numeric_formatting_context")

    if (
        endsWithAny(windowBefore, REVISION_PREFIXES, window=8)
        or (
            (
                endsWithAny(windowBefore, REVISION_ERA_PREFIXES, window=8)
                or endsWithAny(windowBefore, REVISION_SOURCE_TOKENS, window=8)
            )
            and containsAny(localText, REVISION_SOURCE_TOKENS)
            and containsAny(localText, REVISION_ACTION_TOKENS)
        )
    ):
        return markerLocal("revision_note_or_metadata")

    if endsWithAny(windowBefore, NOTE_PREFIXES, window=4):
        return markerLocal("page_or_table_note_marker")
    if beforeStripped.endswith("A") and (span.rawText in {"3", "4"} or span.normalizedText in {"3", "4"}):
        return markerLocal("table_or_appendix_noise")
    if endsWithAny(windowBefore, TABLE_PREFIXES, window=12):
        return markerLocal("table_or_appendix_noise")
    if endsWithAny(windowBefore, ARTICLE_PREFIXES, window=8) and startsWithAny(windowAfter, TABLE_SUFFIXES):
        return markerLocal("table_or_appendix_noise")

    if (
        endsWithAny(windowBefore, FORM_PREFIXES, window=12)
        or (endsWithAny(windowBefore, ARTICLE_PREFIXES, window=8) and startsWithAny(windowAfter, FORM_REVERSED_SUFFIXES))
    ) and startsWithAny(windowAfter, FORM_SUFFIXES):
        return markerLocal("attachment_form_number")
    if endsWithAny(windowBefore, ARTICLE_PREFIXES, window=8) and startsWithAny(windowAfter, ARTICLE_UNITS):
        return markerLocal("article_subnumber_or_citation")
    if endsWithAny(windowBefore, RELATIVE_ARTICLE_PREFIXES, window=8) and startsWithAny(windowAfter, ARTICLE_UNITS):
        return markerLocal("article_subnumber_or_citation")
    if endsWithAny(windowBefore, SUBNUMBER_PREFIXES, window=8):
        return markerLocal("article_subnumber_or_citation")
    if endsWithAny(windowBefore, ERA_PREFIXES, window=8) and startsWithAny(windowAfter, DATE_UNITS):
        return markerLocal("era_date_or_effective_date")
    if startsWithAny(windowAfter, DATE_UNITS):
        return markerLocal("era_date_or_effective_date")
    if beforeStripped.endswith("年") and startsWithAny(windowAfter, ["回"]):
        return markerLocal("quantity_date_unit")
    if startsWithAny(windowAfter, QUANTITY_UNITS):
        return markerLocal("quantity_date_unit")
    if startsWithAny(windowAfter, FRACTION_PARTS) or endsWithAny(windowBefore, FRACTION_PARTS, window=8):
        return markerLocal("quantity_date_unit")
    if endsWithAny(windowBefore, OCR_INTRUSION_PREFIXES, window=4) and startsWithAny(windowAfter, OCR_INTRUSION_SUFFIXES):
        return markerLocal("ocr_or_formatting_noise")
    if before.endswith("(") or before.endswith("（") or after.startswith(")") or after.startswith("）"):
        return markerLocal("parenthetical_or_enumeration_noise")
    if after.startswith(".") or after.startswith("．"):
        return markerLocal("numeric_sequence_or_index")

    return "", [], spanStart, spanEnd, matchedText, negativeSpanId


def detectOcrDemotionEvidence(before: str, after: str) -> str:
    afterStripped = stripLocalSpaces(after)
    if afterStripped == "" or startsWithAny(afterStripped, ["。", "、", ")", "）"]):
        return "ocr_or_formatting_noise_demoted"
    return ""


def detectNearbyLegalReference(row: SentenceRow, span: NumericSpan) -> bool:
    window = row.sentenceText[max(0, span.start - 24) : min(len(row.sentenceText), span.end + 32)]
    return bool(containsAny(window, LEGAL_NEARBY_PATTERNS))


def classifyCandidate(
    row: SentenceRow,
    span: NumericSpan,
    allSpans: Sequence[NumericSpan],
    runBySpan: dict[int, list[NumericSpan]],
    contextLen: int,
) -> Candidate:
    before = row.sentenceText[: span.start]
    after = row.sentenceText[span.end :]
    beforeContext = before[-contextLen:]
    afterContext = after[:contextLen]
    bracketDepth, quoteDepth = resolveBracketQuoteDepth(row.sentenceText, span.start)
    insideBracketOrQuote = bracketDepth > 0 or quoteDepth > 0
    openerPattern = detectNarrowOpener(after)
    precedingPattern = detectPrecedingEvidence(before)
    weakSubject = detectWeakSubject(after)
    negativeType, negativeReasons, negativeStart, negativeEnd, negativeText, negativeSpanId = detectNegativeEvidence(row, span)
    demotionReason = ""
    if not negativeType:
        demotionReason = detectOcrDemotionEvidence(before, after)
        if demotionReason:
            negativeReasons.append(demotionReason)
    positiveReasons: list[str] = []
    targetedExclusionReasons: list[str] = []

    if openerPattern:
        positiveReasons.append("narrow_opener")
    if precedingPattern:
        positiveReasons.append("preceding_boundary")
    if weakSubject:
        positiveReasons.append("short_subject_ending_wa")
    if span.markerValue is not None and span.markerValue >= 2:
        positiveReasons.append("marker_value_later_without_requiring_1")
    if insideBracketOrQuote:
        negativeReasons.append("inside_bracket_or_quote")
        markerIsParentheticalEnumeration = (
            (before.endswith("(") and after.startswith(")"))
            or (before.endswith("（") and after.startswith("）"))
        )
        if negativeType == "parenthetical_or_enumeration_noise" and not markerIsParentheticalEnumeration:
            negativeType = ""
            negativeStart = ""
            negativeEnd = ""
            negativeText = ""
            negativeSpanId = ""
    if row.isTableParagraph or hasMarkdownPipeNoise(row.sentenceText) or hasMarkdownPipeNoise(row.paragraphText):
        negativeReasons.append("table_or_appendix_noise")
    if detectNearbyLegalReference(row, span) and not negativeType:
        negativeReasons.append("legal_reference_nearby_but_not_marker_local")

    runMembers = ""
    if span.spanId in runBySpan:
        members = runBySpan[span.spanId]
        runMembers = json.dumps(
            [{"span_id": member.spanId, "value": member.markerValue, "offset": member.start} for member in members],
            ensure_ascii=False,
        )
        positiveReasons.append("explicit_numeric_run")

    substantivePositiveReasons = [
        reason
        for reason in positiveReasons
        if reason != "marker_value_later_without_requiring_1"
    ]
    sentenceInitial = isSentenceInitial(row, span)
    broadRuleMatched = 1
    narrowOpenerMatched = 1 if openerPattern else 0
    wouldBeTargeted = 1 if openerPattern and not row.isTableParagraph and not sentenceInitial else 0
    collectionStage = "broad_only"
    candidateType = "ambiguous"
    confidence = "low"
    actionability = "review_candidate"

    if row.isTableParagraph or "table_or_appendix_noise" in negativeReasons:
        collectionStage = "table_noise"
        candidateType = "table_or_appendix_noise"
        confidence = "reject"
        actionability = "table_or_appendix"
    elif span.spanShape != "single_digit":
        collectionStage = "numeric_span_noise"
        candidateType = "multi_digit_numeric_span"
        confidence = "reject"
        actionability = "numeric_noise"
    elif negativeType:
        candidateType = negativeType
        confidence = "reject"
        actionability = "reject_marker_local_negative"
        collectionStage = "numeric_span_noise" if span.spanShape != "single_digit" else "broad_only"
    elif demotionReason:
        collectionStage = "broad_only"
        candidateType = "ambiguous"
        confidence = "low"
        actionability = "review_candidate"
    elif sentenceInitial:
        collectionStage = "sentence_initial"
        candidateType = "sentence_initial_numbered_paragraph"
        confidence = "high" if positiveReasons else "medium"
        actionability = "already_split_or_sentence_initial"
    elif span.spanId in runBySpan:
        collectionStage = "explicit_run"
        candidateType = "explicit_run"
        confidence = "high" if not negativeReasons else "medium"
        actionability = "review_candidate"
    elif span.markerValue is not None and span.markerValue >= 2 and substantivePositiveReasons:
        collectionStage = "targeted_opener" if openerPattern else "broad_only"
        candidateType = "implicit_first_paragraph"
        confidence = "medium" if "legal_reference_nearby_but_not_marker_local" in negativeReasons else "high"
        actionability = "review_candidate"
    elif substantivePositiveReasons:
        collectionStage = "targeted_opener" if openerPattern else "broad_only"
        candidateType = "ambiguous"
        confidence = "low"
        actionability = "review_candidate"

    if insideBracketOrQuote and confidence != "reject":
        confidence = "low"
        actionability = "inside_bracket_or_quote_review"

    if not openerPattern:
        targetedExclusionReasons.append("no_narrow_opener")
    if negativeReasons:
        targetedExclusionReasons.extend(negativeReasons)
    if span.spanShape != "single_digit":
        targetedExclusionReasons.append(span.spanShape)

    if insideBracketOrQuote and confidence != "reject":
        auditGroup = "inside_bracket_or_quote"
    elif confidence == "reject":
        firstReason = negativeReasons[0] if negativeReasons else candidateType
        auditGroup = f"reject_{firstReason}"
    elif collectionStage == "targeted_opener":
        auditGroup = f"targeted_{confidence}"
    else:
        auditGroup = f"broad_only_{confidence}"

    markerGlobalOffset = ""
    paragraphCharOffset = ""
    if row.sentenceCharOffsetInParagraph >= 0:
        markerGlobalOffset = str(row.sentenceCharOffsetInParagraph + span.start)
        paragraphCharOffset = markerGlobalOffset

    paragraphSampleStart = max(0, (int(paragraphCharOffset) if paragraphCharOffset else 0) - contextLen)
    paragraphSampleEnd = min(len(row.paragraphText), (int(paragraphCharOffset) if paragraphCharOffset else 0) + contextLen)

    return Candidate(
        candidate_id=f"{row.sentenceId}:{span.spanId}",
        run_id=row.runId,
        sentence_id=row.sentenceId,
        paragraph_id=row.paragraphId,
        document_id=row.documentId,
        file_name=row.fileName,
        source_file_path=row.sourceFilePath,
        is_table_paragraph=row.isTableParagraph,
        sentence_no_in_document=row.sentenceNoInDocument,
        sentence_no_in_paragraph=row.sentenceNoInParagraph,
        marker=span.rawText,
        marker_value="" if span.markerValue is None else str(span.markerValue),
        offset=span.start,
        candidate_type=candidateType,
        confidence=confidence,
        actionability=actionability,
        split_decision="report_only",
        before_context=beforeContext,
        after_context=afterContext,
        sentence_text=row.sentenceText,
        paragraph_text_sample=row.paragraphText[paragraphSampleStart:paragraphSampleEnd],
        positive_reasons=";".join(sorted(set(positiveReasons))),
        negative_reasons=";".join(sorted(set(negativeReasons))),
        opener_pattern=openerPattern,
        preceding_pattern=precedingPattern,
        following_pattern=weakSubject,
        run_members=runMembers,
        broad_rule_matched=broadRuleMatched,
        narrow_opener_matched=narrowOpenerMatched,
        would_be_in_targeted_135=wouldBeTargeted,
        targeted_exclusion_reasons=";".join(sorted(set(targetedExclusionReasons))),
        collection_stage=collectionStage,
        audit_group=auditGroup,
        matched_negative_span_start=negativeStart,
        matched_negative_span_end=negativeEnd,
        matched_negative_text=negativeText,
        negative_span_id=negativeSpanId,
        marker_global_offset_in_paragraph=markerGlobalOffset,
        sentence_char_offset_in_paragraph=str(row.sentenceCharOffsetInParagraph),
        paragraph_char_offset=paragraphCharOffset,
        offset_resolution_reason=row.offsetResolutionReason,
        previous_sentence_text=row.previousSentenceText,
        next_sentence_text=row.nextSentenceText,
        before_context_len=len(beforeContext),
        after_context_len=len(afterContext),
        numeric_span_id=span.spanId,
        numeric_span_start=span.start,
        numeric_span_end=span.end,
        numeric_span_text=span.rawText,
        numeric_span_normalized_text=span.normalizedText,
        numeric_span_shape=span.spanShape,
        paragraph_full_text=row.paragraphText,
    )


def collectCandidatesFromRow(row: SentenceRow, contextLen: int = 80) -> list[Candidate]:
    spans = tokenizeNumericSpans(row.sentenceText)
    runBySpan = detectExplicitRun(spans)
    return [classifyCandidate(row, span, spans, runBySpan, contextLen) for span in spans]


def missingRequiredTables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    existing = {row[0] for row in rows}
    required = ["analysis_sentences", "analysis_paragraphs", "analysis_documents"]
    return [tableName for tableName in required if tableName not in existing]


def tableColumns(conn: sqlite3.Connection, tableName: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({tableName})").fetchall()}


def resolveRunId(conn: sqlite3.Connection, requestedRunId: Optional[int]) -> Optional[int]:
    sentenceColumns = tableColumns(conn, "analysis_sentences")
    if "run_id" not in sentenceColumns:
        return None
    if requestedRunId is not None:
        return requestedRunId
    existingTables = {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    }
    if "analysis_runs" in existingTables:
        run = conn.execute(
            """
            SELECT run_id
            FROM analysis_runs
            WHERE status = 'completed'
            ORDER BY run_id DESC
            LIMIT 1
            """
        ).fetchone()
        if run is not None:
            return int(run[0])
    run = conn.execute("SELECT MAX(run_id) FROM analysis_sentences").fetchone()
    if run is None or run[0] is None:
        return None
    return int(run[0])


def loadSentenceRows(
    conn: sqlite3.Connection,
    limit: Optional[int] = None,
    runId: Optional[int] = None,
) -> list[SentenceRow]:
    sentenceColumns = tableColumns(conn, "analysis_sentences")
    paragraphColumns = tableColumns(conn, "analysis_paragraphs")
    documentColumns = tableColumns(conn, "analysis_documents")
    hasRunId = (
        "run_id" in sentenceColumns
        and "run_id" in paragraphColumns
        and "run_id" in documentColumns
    )
    if hasRunId and runId is None:
        runId = resolveRunId(conn, None)
    runSelect = "s.run_id AS selected_run_id" if "run_id" in sentenceColumns else "0 AS selected_run_id"
    paragraphRunClause = "AND p.run_id = s.run_id" if hasRunId else ""
    documentRunClause = "AND d.run_id = s.run_id" if hasRunId else ""
    whereClause = "WHERE s.run_id = ?" if hasRunId and runId is not None else ""
    params: list[int] = []
    if hasRunId and runId is not None:
        params.append(runId)

    query = f"""
        SELECT
            {runSelect},
            s.sentence_id,
            s.paragraph_id,
            s.document_id,
            d.file_name,
            d.source_file_path,
            p.is_table_paragraph,
            s.sentence_no_in_document,
            s.sentence_no_in_paragraph,
            s.sentence_text,
            p.paragraph_text
        FROM analysis_sentences AS s
        JOIN analysis_paragraphs AS p ON p.paragraph_id = s.paragraph_id {paragraphRunClause}
        JOIN analysis_documents AS d ON d.document_id = s.document_id {documentRunClause}
        {whereClause}
        ORDER BY selected_run_id, s.document_id, s.sentence_no_in_document
    """
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
        rawRows = conn.execute(query, params).fetchall()
    else:
        rawRows = conn.execute(query, params).fetchall()

    rows = [
        SentenceRow(
            runId=int(raw[0] or 0),
            sentenceId=int(raw[1]),
            paragraphId=int(raw[2]),
            documentId=int(raw[3]),
            fileName=str(raw[4]),
            sourceFilePath=str(raw[5]),
            isTableParagraph=int(raw[6] or 0),
            sentenceNoInDocument=int(raw[7]),
            sentenceNoInParagraph=int(raw[8]),
            sentenceText=str(raw[9] or ""),
            paragraphText=str(raw[10] or ""),
        )
        for raw in rawRows
    ]

    for index, row in enumerate(rows):
        if (
            index > 0
            and rows[index - 1].runId == row.runId
            and rows[index - 1].documentId == row.documentId
        ):
            row.previousSentenceText = rows[index - 1].sentenceText
        if (
            index + 1 < len(rows)
            and rows[index + 1].runId == row.runId
            and rows[index + 1].documentId == row.documentId
        ):
            row.nextSentenceText = rows[index + 1].sentenceText

    resolveSentenceOffsets(rows)
    return rows


def resolveSentenceOffsets(rows: Sequence[SentenceRow]) -> None:
    nextSearchStartByParagraph: dict[tuple[int, int], int] = defaultdict(int)
    for row in rows:
        paragraphKey = (row.runId, row.paragraphId)
        startAt = nextSearchStartByParagraph[paragraphKey]
        offset = row.paragraphText.find(row.sentenceText, startAt)
        allOccurrences = findAllOccurrences(row.paragraphText, row.sentenceText)
        if offset >= 0:
            row.sentenceCharOffsetInParagraph = offset
            nextSearchStartByParagraph[paragraphKey] = offset + len(row.sentenceText)
            row.offsetResolutionReason = "ordered_search_duplicate_text" if len(allOccurrences) > 1 else "ordered_search_unique"
            continue
        fallbackOffset = row.paragraphText.find(row.sentenceText)
        if fallbackOffset >= 0:
            row.sentenceCharOffsetInParagraph = fallbackOffset
            row.offsetResolutionReason = "fallback_search_duplicate_text" if len(allOccurrences) > 1 else "fallback_search_unique"
        else:
            row.sentenceCharOffsetInParagraph = -1
            row.offsetResolutionReason = "sentence_text_not_found_in_paragraph"


def findAllOccurrences(text: str, needle: str) -> list[int]:
    if not needle:
        return []
    offsets: list[int] = []
    start = 0
    while True:
        offset = text.find(needle, start)
        if offset < 0:
            return offsets
        offsets.append(offset)
        start = offset + max(1, len(needle))


def collectCandidates(rows: Sequence[SentenceRow], contextLen: int = 80) -> list[Candidate]:
    candidates: list[Candidate] = []
    for row in rows:
        if not row.sentenceText:
            continue
        candidates.extend(collectCandidatesFromRow(row, contextLen))
    return candidates


def splitReasons(value: str) -> list[str]:
    return [part for part in value.split(";") if part]


def buildSummary(candidates: Sequence[Candidate], selectedRunId: Optional[int] = None) -> dict[str, object]:
    byType = Counter(candidate.candidate_type for candidate in candidates)
    byConfidence = Counter(candidate.confidence for candidate in candidates)
    byNegative = Counter(reason for candidate in candidates for reason in splitReasons(candidate.negative_reasons))
    byPositive = Counter(reason for candidate in candidates for reason in splitReasons(candidate.positive_reasons))
    byFile = Counter(candidate.file_name for candidate in candidates)
    highMediumByFile = Counter(
        candidate.file_name
        for candidate in candidates
        if candidate.confidence in {"high", "medium"}
    )
    broadOnly = [candidate for candidate in candidates if candidate.collection_stage == "broad_only"]
    targeted = [candidate for candidate in candidates if candidate.narrow_opener_matched]

    return {
        "selected_run_id": selectedRunId,
        "candidate_run_ids": sorted({candidate.run_id for candidate in candidates}),
        "total_candidates": len(candidates),
        "total_broad_candidates": sum(candidate.broad_rule_matched for candidate in candidates),
        "total_targeted_opener_candidates": len(targeted),
        "total_broad_only_candidates": len(broadOnly),
        "by_candidate_type": dict(byType),
        "by_confidence": dict(byConfidence),
        "by_negative_reason": dict(byNegative),
        "by_positive_reason": dict(byPositive),
        "top_files_by_candidate_count": byFile.most_common(20),
        "top_files_by_high_medium_count": highMediumByFile.most_common(20),
        "broad_only_by_confidence": dict(Counter(candidate.confidence for candidate in broadOnly)),
        "broad_only_by_negative_reason": dict(
            Counter(reason for candidate in broadOnly for reason in splitReasons(candidate.negative_reasons))
        ),
        "sample_high": sampleCandidates(candidates, lambda candidate: candidate.confidence == "high"),
        "sample_medium": sampleCandidates(candidates, lambda candidate: candidate.confidence == "medium"),
        "sample_low": sampleCandidates(candidates, lambda candidate: candidate.confidence == "low"),
        "sample_reject_by_reason": sampleRejectsByReason(candidates),
        "sample_broad_only_high": sampleCandidates(broadOnly, lambda candidate: candidate.confidence == "high"),
        "sample_broad_only_medium": sampleCandidates(broadOnly, lambda candidate: candidate.confidence == "medium"),
        "sample_broad_only_low": sampleCandidates(broadOnly, lambda candidate: candidate.confidence == "low"),
        "sample_broad_only_reject": sampleCandidates(broadOnly, lambda candidate: candidate.confidence == "reject"),
        "sample_marker_value_2_without_1": sampleCandidates(
            candidates,
            lambda candidate: candidate.marker_value == "2" and "marker_value_later_without_requiring_1" in candidate.positive_reasons,
        ),
        "sample_later_marker_without_prior_1": sampleCandidates(
            candidates,
            lambda candidate: candidate.marker_value not in {"", "1"} and "marker_value_later_without_requiring_1" in candidate.positive_reasons,
        ),
    }


def sampleCandidates(candidates: Sequence[Candidate], predicate, limit: int = 10) -> list[dict[str, object]]:
    samples: list[dict[str, object]] = []
    for candidate in candidates:
        if not predicate(candidate):
            continue
        samples.append(sampleCandidate(candidate))
        if len(samples) >= limit:
            break
    return samples


def sampleCandidate(candidate: Candidate) -> dict[str, object]:
    return {
        "candidate_id": candidate.candidate_id,
        "file_name": candidate.file_name,
        "marker": candidate.marker,
        "marker_value": candidate.marker_value,
        "candidate_type": candidate.candidate_type,
        "confidence": candidate.confidence,
        "audit_group": candidate.audit_group,
        "positive_reasons": candidate.positive_reasons,
        "negative_reasons": candidate.negative_reasons,
        "context": f"{candidate.before_context}[{candidate.marker}]{candidate.after_context}",
    }


def sampleRejectsByReason(candidates: Sequence[Candidate], limitPerReason: int = 5) -> dict[str, list[dict[str, object]]]:
    samples: dict[str, list[dict[str, object]]] = defaultdict(list)
    for candidate in candidates:
        if candidate.confidence != "reject":
            continue
        reasons = splitReasons(candidate.negative_reasons) or [candidate.candidate_type]
        for reason in reasons:
            if len(samples[reason]) < limitPerReason:
                samples[reason].append(sampleCandidate(candidate))
    return dict(samples)


def writeCsv(path: Path, candidates: Sequence[Candidate]) -> None:
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for candidate in candidates:
            row = asdict(candidate)
            row.pop("paragraph_full_text", None)
            writer.writerow(row)


def writeJsonl(path: Path, candidates: Sequence[Candidate]) -> None:
    with path.open("w", encoding="utf-8") as output:
        for candidate in candidates:
            output.write(json.dumps(asdict(candidate), ensure_ascii=False) + "\n")


def writeSummary(path: Path, summary: dict[str, object]) -> None:
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


def writeSamples(path: Path, summary: dict[str, object]) -> None:
    lines = ["# Paragraph Number Candidate Samples", ""]
    sampleKeys = [
        "sample_high",
        "sample_medium",
        "sample_low",
        "sample_broad_only_high",
        "sample_broad_only_medium",
        "sample_broad_only_low",
        "sample_broad_only_reject",
        "sample_marker_value_2_without_1",
        "sample_later_marker_without_prior_1",
    ]
    for key in sampleKeys:
        lines.extend([f"## {key}", ""])
        for sample in summary.get(key, []):
            lines.extend(formatSample(sample))
        lines.append("")

    lines.extend(["## sample_reject_by_reason", ""])
    rejectSamples = summary.get("sample_reject_by_reason", {})
    if isinstance(rejectSamples, dict):
        for reason, samples in rejectSamples.items():
            lines.extend([f"### {reason}", ""])
            for sample in samples:
                lines.extend(formatSample(sample))
            lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def formatSample(sample: dict[str, object]) -> list[str]:
    return [
        f"- `{sample.get('candidate_id')}` `{sample.get('candidate_type')}` `{sample.get('confidence')}` `{sample.get('audit_group')}`",
        f"  - file: `{sample.get('file_name')}`",
        f"  - reasons: +`{sample.get('positive_reasons')}` -`{sample.get('negative_reasons')}`",
        f"  - context: {sample.get('context')}",
    ]


def writeReports(outDir: Path, candidates: Sequence[Candidate], selectedRunId: Optional[int] = None) -> None:
    outDir.mkdir(parents=True, exist_ok=True)
    reviewCandidates = [
        candidate
        for candidate in candidates
        if candidate.confidence in {"high", "medium", "low"}
    ]
    summary = buildSummary(candidates, selectedRunId)
    writeCsv(outDir / "paragraph_number_candidates.all.csv", candidates)
    writeCsv(outDir / "paragraph_number_candidates.review.csv", reviewCandidates)
    writeJsonl(outDir / "paragraph_number_candidates.all.jsonl", candidates)
    writeSummary(outDir / "paragraph_number_candidates.summary.json", summary)
    writeSamples(outDir / "paragraph_number_candidates.samples.md", summary)


def run(argv: Optional[Sequence[str]] = None) -> int:
    args = parseArgs(argv)
    analysisDb = Path(args.analysis_db)
    if not analysisDb.exists():
        raise SystemExit(f"analysis DB not found: {analysisDb}")

    dbUri = f"file:{analysisDb.as_posix()}?mode=ro"
    with sqlite3.connect(dbUri, uri=True) as conn:
        missingTables = missingRequiredTables(conn)
        if missingTables:
            raise SystemExit(
                "analysis DB is missing required analysis tables: "
                + ", ".join(missingTables)
            )
        runId = resolveRunId(conn, args.run_id)
        rows = loadSentenceRows(conn, args.limit, runId)
    candidates = collectCandidates(rows, args.context_len)
    writeReports(Path(args.out_dir), candidates, runId)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    return run(argv)


if __name__ == "__main__":
    raise SystemExit(main())
