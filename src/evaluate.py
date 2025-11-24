# src/votebot_evaluate_single_proposal_and_vote.py
"""
Production-ready evaluator for CyberGov V0 (Firestore backend, OpenRouter/Gemini models).

Usage (locally / CI):
    python src/votebot_evaluate_single_proposal_and_vote.py <network> <proposal_id>

Environment variables (recommended to set as GitHub Secrets in Actions):
    - OPENROUTER_API_KEY           (required) : key for OpenRouter (used to call Gemini / other models)
    - FIREBASE_CREDENTIALS_JSON    (required) : full JSON service account as string OR path in FIREBASE_SA_PATH
    - GITHUB_REPOSITORY           (optional) : used in manifest provenance
    - GITHUB_RUN_ID               (optional)
    - GITHUB_SHA                  (optional)
"""
import sys
import json
import datetime
import os
import hashlib
from pathlib import Path
from collections import Counter
from typing import Dict, List, Tuple, Optional

# Firestore
import firebase_admin
from firebase_admin import credentials as fb_credentials
from firebase_admin import firestore as admin_firestore

# Internal utils - expect these to exist in your codebase
from utils.helpers import setup_logging, get_config_from_env, hash_file
from utils.run_magi_eval import run_single_inference, setup_compiled_agent
from utils.weighted_decision_engine import compute_weighted_decision, list_available_templates

logger = setup_logging()

MAGI_MODELS_DEFAULT = magi_models = {
    "balthazar": "gemini-2.5-flash",  # or the Gemini model you're using
    "melchior": "gemini-2.5-flash",
    "caspar": "gemini-2.5-flash",
}



MAGI_NAMES = ["balthazar", "melchior", "caspar"]

# ---------------------------
# Firestore helpers
# ---------------------------
def initialize_firebase_app_from_env() -> None:
    """
    Initialize firebase_admin app idempotently.
    Accepts either:
      - FIREBASE_CREDENTIALS_JSON env var (JSON string) or
      - FIREBASE_SA_PATH env var pointing to a file path to a JSON file.
    """
    if firebase_admin._apps:
        return

    # Prefer direct JSON string first (useful for GitHub Actions secret)
    raw = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if raw:
        try:
            # If it's a JSON string, parse it. If it's a path, handle below.
            # Accept triple-quoted and quoted secrets (strip wrappers).
            cleaned = raw.strip()
            if (cleaned.startswith('"""') and cleaned.endswith('"""')) or (
                cleaned.startswith("'''") and cleaned.endswith("'''")
            ):
                cleaned = cleaned[3:-3].strip()
            # Some CI systems double-quote the whole JSON - strip outer quotes if present
            if cleaned.startswith('"') and cleaned.endswith('"'):
                cleaned = cleaned[1:-1]
            creds_dict = json.loads(cleaned)
            cred = fb_credentials.Certificate(creds_dict)
            firebase_admin.initialize_app(cred)
            logger.info("Initialized Firebase app using FIREBASE_CREDENTIALS_JSON.")
            return
        except Exception as e:
            logger.error("Failed parsing FIREBASE_CREDENTIALS_JSON: %s", e)
            raise

    # fallback: path
    sa_path = os.getenv("FIREBASE_SA_PATH")
    if sa_path and Path(sa_path).exists():
        cred = fb_credentials.Certificate(str(sa_path))
        firebase_admin.initialize_app(cred)
        logger.info("Initialized Firebase app using FIREBASE_SA_PATH.")
        return

    # final fallback: try default application credentials
    try:
        firebase_admin.initialize_app()
        logger.info("Initialized Firebase app using default credentials (no explicit SA provided).")
        return
    except Exception as e:
        logger.error("No valid Firebase credentials found. Set FIREBASE_CREDENTIALS_JSON or FIREBASE_SA_PATH.")
        raise RuntimeError("Firebase credentials missing") from e


def get_firestore_client():
    """Return Firestore client (assumes firebase_admin initialized)."""
    return admin_firestore.client()


# ---------------------------
# Load Magi personalities
# ---------------------------
def load_magi_personalities() -> Dict[str, str]:
    """
    Load system prompts from templates/system_prompts/<name>_system_prompt.md.
    Returns dict mapping magi name -> prompt content.
    """
    personalities: Dict[str, str] = {}
    base_dir = Path(__file__).parent.parent / "templates" / "system_prompts"
    for name in MAGI_NAMES:
        p = base_dir / f"{name}_system_prompt.md"
        if p.exists():
            try:
                personalities[name] = p.read_text(encoding="utf-8").strip()
                logger.info("Loaded system prompt: %s", p)
            except Exception as e:
                logger.error("Failed reading prompt %s: %s", p, e)
                personalities[name] = ""
        else:
            logger.warning("System prompt missing (expected): %s", p)
            personalities[name] = ""
    return personalities


# ---------------------------
# Summary rationale generator
# ---------------------------
def generate_summary_rationale(
    votes_breakdown: List[Dict], proposal_id: int, network: str, analysis_files: List[Path]
) -> str:
    """
    Create a simple HTML summary combining the rationales. Stores minimal links referencing GitHub run.
    """
    github_run_id = os.getenv("GITHUB_RUN_ID", "N/A")
    aye_votes = sum(1 for v in votes_breakdown if v["decision"].upper() == "AYE")
    nay_votes = sum(1 for v in votes_breakdown if v["decision"].upper() == "NAY")
    abstain_votes = sum(1 for v in votes_breakdown if v["decision"].upper() == "ABSTAIN")

    # try extract rationales from files if available
    rationales = {name: None for name in MAGI_NAMES}
    decisions = {name: None for name in MAGI_NAMES}
    for af in analysis_files:
        try:
            data = json.loads(af.read_text(encoding="utf-8"))
            nm = af.stem
            if nm in rationales:
                rationales[nm] = data.get("rationale", "")
                decisions[nm] = data.get("decision", "")
        except Exception:
            continue

    summary_text = f"""
<p>A panel of autonomous agents reviewed this proposal, resulting in a vote of <strong>{aye_votes} AYE</strong>, <strong>{nay_votes} NAY</strong>, and <strong>{abstain_votes} ABSTAIN</strong>.</p>
"""
    for name in MAGI_NAMES:
        summary_text += f"<h3>{name.title()} voted <u>{decisions.get(name) or 'N/A'}</u></h3>\n"
        summary_text += f"<blockquote>{rationales.get(name) or 'Rationale not available.'}</blockquote>\n"

    summary_text += f"""
<h3>System Transparency</h3>
<p>Manifest and outputs are stored in Firestore under this proposal document. GitHub run id: {github_run_id}.</p>
"""
    return summary_text


# ---------------------------
# Firestore preflight checks (uses Firestore rather than S3)
# ---------------------------
def perform_preflight_checks_firestore(db, proposal_doc_ref) -> Tuple[List[Dict], Path, List[str]]:
    """
    Checks Firestore proposal doc for required fields and writes content file locally.
    Returns (manifest_inputs, local_content_path, magi_models_list)
    """
    logger.info("01 - Performing pre-flight checks (Firestore)...")
    manifest_inputs: List[Dict] = []

    snap = proposal_doc_ref.get()
    if not snap.exists:
        logger.error("Firestore proposal doc does NOT exist: %s", proposal_doc_ref.id)
        raise RuntimeError("Firestore proposal doc missing")

    doc = snap.to_dict()
    files = doc.get("files", {}) or {}

    # rawData
    raw_data = files.get("rawData") or doc.get("rawData")
    if not raw_data:
        logger.error("rawData not found in Firestore doc")
        raise RuntimeError("rawData missing")

    local_workspace = Path("workspace")
    local_workspace.mkdir(exist_ok=True)

    local_raw_path = local_workspace / "polkassembly.json"
    local_raw_path.write_text(json.dumps(raw_data, indent=2), encoding="utf-8")
    manifest_inputs.append(
        {"logical_name": "polkassembly", "firestore_path": f"proposals/{proposal_doc_ref.id}/files/rawData", "hash": hash_file(local_raw_path)}
    )
    logger.info("✅ raw_polkassembly.json validated and saved locally.")

    # content (generated by scraper)
    content = files.get("content") or doc.get("content") or raw_data.get("content") or raw_data.get("body")
    if not content:
        logger.error("content markdown not found in Firestore doc")
        raise RuntimeError("content missing")

    local_content_path = local_workspace / "content.md"
    local_content_path.write_text(content, encoding="utf-8")
    manifest_inputs.append(
        {"logical_name": "content_markdown", "firestore_path": f"proposals/{proposal_doc_ref.id}/files/content", "hash": hash_file(local_content_path)}
    )
    logger.info("✅ content.md saved locally.")

    # system prompts - check existence
    prompt_dir = Path("templates/system_prompts")
    magi_models_list = MAGI_NAMES.copy()
    for name in magi_models_list:
        p = prompt_dir / f"{name}_system_prompt.md"
        if not p.exists():
            logger.warning("System prompt missing: %s (will continue but prompt will be empty)", p)
    logger.info("Pre-flight checks done.")
    return manifest_inputs, local_content_path, magi_models_list


# ---------------------------
# Run MAGI evaluations
# ---------------------------
def run_magi_evaluations_firestore(magi_models_list: List[str], local_workspace: Path) -> List[Path]:
    """
    For each magi name:
      - use configured model string (MAGI_MODELS_DEFAULT)
      - compile agent via setup_compiled_agent(model_id=model_string)
      - run run_single_inference(compiled_agent, prompt, proposal_text)
      - save analysis JSON locally
      - return list of analysis file Paths
    """
    logger.info("02 - Running MAGI evaluations...")
    analysis_dir = local_workspace / "llm_analyses"
    analysis_dir.mkdir(exist_ok=True)

    personalities = load_magi_personalities()
    # Allow environment override of model mapping if present (JSON string in env)
    magi_llms = MAGI_MODELS_DEFAULT.copy()
    env_map = os.getenv("CYBERGOV_MAGI_MODELS_JSON")
    if env_map:
        try:
            parsed = json.loads(env_map)
            magi_llms.update(parsed)
            logger.info("Using model overrides from CYBERGOV_MAGI_MODELS_JSON")
        except Exception:
            logger.warning("Failed to parse CYBERGOV_MAGI_MODELS_JSON; using defaults")

    proposal_content_path = local_workspace / "content.md"
    if not proposal_content_path.exists():
        logger.error("Proposal content.md missing at %s", proposal_content_path)
        raise RuntimeError("content.md missing")

    proposal_text = proposal_content_path.read_text(encoding="utf-8")
    output_files: List[Path] = []
    for magi_key in magi_models_list:
        if magi_key not in magi_llms:
            logger.warning("Skipping %s: no model mapped", magi_key)
            continue

        model_id = magi_llms[magi_key]
        personality_prompt = personalities.get(magi_key, "")

        logger.info("--- Processing Magi: %s (model=%s) ---", magi_key, model_id)

        # Compile agent (this is your existing helper; ensure it supports openrouter model strings)
        compiled_agent = setup_compiled_agent(model_id=model_id)
        prediction = run_single_inference(compiled_agent, personality_prompt, proposal_text)

        # Prepare output JSON (rich fields)
        out_path = analysis_dir / f"{magi_key}.json"
        raw_api_response = getattr(prediction, "raw_api_response", None) or getattr(prediction, "raw_response", None) or {}

        data = {
            "model_name": model_id,
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "decision": getattr(prediction, "vote", "").strip() or getattr(prediction, "decision", ""),
            "confidence": getattr(prediction, "confidence", None),
            "rationale": getattr(prediction, "rationale", "").strip() or getattr(prediction, "explanation", ""),
            # structured transparency fields (if present)
            "critical_analysis": getattr(prediction, "critical_analysis", None),
            "factors_considered": getattr(prediction, "factors_considered", None),
            "scores": getattr(prediction, "scores", None),
            "decision_trace": getattr(prediction, "decision_trace", None),
            "safety_flags": getattr(prediction, "safety_flags", None),
            "raw_api_response": raw_api_response,
        }

        out_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        output_files.append(out_path)
        logger.info("✅ Generated analysis for %s -> %s", magi_key, out_path.name)

    return output_files


# ---------------------------
# Consolidate votes using Weighted Decision Engine
# ---------------------------
def consolidate_vote(analysis_files: List[Path], local_workspace: Path, proposal_id: int, network: str) -> Path:
    logger.info("03 - Consolidating vote using Weighted Decision Engine...")
    
    # Get strategy from environment variable (default: neutral)
    strategy_name = os.getenv("CYBERGOV_VOTING_STRATEGY", "neutral").lower()
    logger.info(f"Using voting strategy: {strategy_name}")
    
    # Prepare agent votes for weighted engine
    agent_votes = []
    votes_breakdown = []
    
    for analysis_file in analysis_files:
        data = json.loads(analysis_file.read_text(encoding="utf-8"))
        agent_name = analysis_file.stem  # balthazar, melchior, caspar
        raw_decision = str(data.get("decision", "")).strip()
        
        # Normalize decision
        if raw_decision.upper() == "AYE":
            normalized = "Aye"
        elif raw_decision.upper() == "NAY":
            normalized = "Nay"
        else:
            normalized = "Abstain"
        
        # For weighted engine
        agent_votes.append({
            "agent": agent_name,
            "decision": normalized
        })
        
        # For breakdown display
        votes_breakdown.append({
            "model": agent_name,
            "decision": normalized,
            "confidence": data.get("confidence")
        })
    
    # Use weighted decision engine
    if not agent_votes:
        logger.warning("No agent votes found, defaulting to Abstain")
        final_decision = "Abstain"
        is_conclusive = False
        is_unanimous = False
        weighted_result = None
    else:
        weighted_result = compute_weighted_decision(
            agent_votes=agent_votes,
            template_name=strategy_name
        )
        
        final_decision = weighted_result.final_decision
        is_unanimous = len(set(v["decision"] for v in agent_votes)) == 1
        is_conclusive = weighted_result.confidence in ["Strong", "Moderate"]
        
        logger.info(f"Weighted Decision: {final_decision}")
        logger.info(f"Confidence: {weighted_result.confidence}")
        logger.info(f"Margin: {weighted_result.margin:.3f}")
        logger.info(f"Rules Triggered: {', '.join(weighted_result.rules_triggered)}")
        logger.info(f"Weighted Scores: Aye={weighted_result.weighted_scores['Aye']:.3f}, "
                   f"Nay={weighted_result.weighted_scores['Nay']:.3f}, "
                   f"Abstain={weighted_result.weighted_scores['Abstain']:.3f}")
    
    summary_rationale = generate_summary_rationale(votes_breakdown, proposal_id, network, analysis_files)
    
    # Build vote data with weighted engine metadata
    vote_data = {
        "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "is_conclusive": is_conclusive,
        "final_decision": final_decision,
        "is_unanimous": is_unanimous,
        "summary_rationale": summary_rationale,
        "votes_breakdown": votes_breakdown,
    }
    
    # Add weighted engine details if available
    if weighted_result:
        vote_data["weighted_decision_metadata"] = {
            "engine_version": weighted_result.engine_version,
            "strategy_used": weighted_result.template_used,
            "template_weights": weighted_result.template_weights,
            "weighted_scores": weighted_result.weighted_scores,
            "margin": weighted_result.margin,
            "confidence": weighted_result.confidence,
            "rules_triggered": weighted_result.rules_triggered,
            "decision_reasoning": weighted_result.decision_reasoning,
            "agent_votes_with_weights": weighted_result.agent_votes
        }
    
    vote_path = local_workspace / "vote.json"
    vote_path.write_text(json.dumps(vote_data, indent=2), encoding="utf-8")
    logger.info("✅ Vote consolidated into %s", vote_path)
    return vote_path


# ---------------------------
# Upload outputs and build manifest (Firestore)
# ---------------------------
def upload_outputs_and_generate_manifest_firestore(
    db, proposal_doc_ref, local_workspace: Path, analysis_files: List[Path], vote_file: Optional[Path], manifest_inputs: List[Dict]
) -> Dict:
    logger.info("04 - Attesting and uploading outputs to Firestore...")
    manifest_outputs: List[Dict] = []

    # Upload each analysis file and vote file (if present) into doc.files.outputs.<logical>
    all_files = analysis_files.copy()
    if vote_file:
        all_files.append(vote_file)
    
    for lf in all_files:
        lf = Path(lf)
        content = lf.read_text(encoding="utf-8")
        file_hash = hash_file(lf)
        logical = lf.stem

        # Write content + metadata under a structured path in the document
        update_payload = {
            f"files.outputs.{logical}.content": content,
            f"files.outputs.{logical}.hash": file_hash,
            f"files.outputs.{logical}.timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        try:
            proposal_doc_ref.update(update_payload)
            logger.info("Uploaded %s -> Firestore path files.outputs.%s", lf.name, logical)
        except Exception as e:
            logger.error("Failed uploading %s: %s", lf.name, e, exc_info=True)
            raise

        manifest_outputs.append({"logical_name": logical, "firestore_path": f"proposals/{proposal_doc_ref.id}/files/outputs/{logical}", "hash": file_hash})

    # Build manifest
    manifest = {
        "provenance": {
            "job_name": "LLM Inference and Voting",
            "github_repository": os.getenv("GITHUB_REPOSITORY", "N/A"),
            "github_run_id": os.getenv("GITHUB_RUN_ID", "N/A"),
            "github_commit_sha": os.getenv("GITHUB_SHA", "N/A"),
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        },
        "inputs": manifest_inputs,
        "outputs": manifest_outputs,
    }

    # canonicalize & hash manifest
    canonical_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode("utf-8")
    canonical_manifest_sha256 = hashlib.sha256(canonical_manifest).hexdigest()
    manifest["canonical_sha256"] = canonical_manifest_sha256

    # save manifest locally and to Firestore
    manifest_path = local_workspace / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    proposal_doc_ref.update({
        "files.manifest": manifest,
        "files.manifest_hash": canonical_manifest_sha256,
        "files.manifest_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    })
    logger.info("✅ Manifest written to Firestore and local workspace. sha256=%s", canonical_manifest_sha256)

    return manifest


# ---------------------------
# Main entrypoint
# ---------------------------
def main():
    logger.info("CyberGov V0 evaluator (Firestore) starting...")
    last_step = "initializing"

    try:
        # config = get_config_from_env()  # optional: keep if you want env-based config helper
        # But for this script we accept network + proposal_id via CLI args
        if len(sys.argv) != 3:
            print("Usage: python src/votebot_evaluate_single_proposal_and_vote.py <network> <proposal_id>")
            sys.exit(1)

        network = sys.argv[1]
        proposal_id = int(sys.argv[2])
        doc_id = f"{network}-{proposal_id}"

        # Initialize Firebase
        initialize_firebase_app_from_env()
        db = get_firestore_client()
        proposal_doc_ref = db.collection("proposals").document(doc_id)

        last_step = "preflight"
        manifest_inputs, local_content_path, magi_models = perform_preflight_checks_firestore(db, proposal_doc_ref)
        local_workspace = local_content_path.parent

        last_step = "magi_eval"
        # Ensure OPENROUTER_API_KEY is provided
        '''
        if not os.getenv("OPENROUTER_API_KEY"):
            logger.error("OPENROUTER_API_KEY missing. Set as env var / GitHub secret.")
            raise RuntimeError("OPENROUTER_API_KEY missing")
'''
        analysis_files = run_magi_evaluations_firestore(magi_models, local_workspace)
        
        # REMOVED: consolidate_vote step (as per "Evaluate Once, Vote Many" architecture)
        # We no longer create a single "vote.json" for the proposal. 
        # Instead, individual user votes are calculated on-demand or via user-specific pipelines.
        vote_file = None 
        
        last_step = "upload"
        manifest = upload_outputs_and_generate_manifest_firestore(db, proposal_doc_ref, local_workspace, analysis_files, vote_file, manifest_inputs)

        logger.info("🎉 Evaluation complete. Manifest SHA256: %s", manifest.get("canonical_sha256"))
        logger.info("Firestore document: proposals/%s", doc_id)

    except Exception as e:
        logger.error("💥 Error during evaluation. Last successful step: %s", last_step, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
