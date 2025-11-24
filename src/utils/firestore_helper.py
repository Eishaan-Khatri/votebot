## src/utils/firestore_helper.py

import json
from typing import Dict, Any, Optional
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
from prefect import get_run_logger
from .weighted_decision_engine import WeightedDecisionResult

class FirestoreHelper:
    def __init__(self, credentials_path: str = None, credentials_dict: dict = None):
        """Initialize Firestore client."""
        import os
        import json
        
        if not firebase_admin._apps:
            # Priority 1: Explicit arguments
            if credentials_dict:
                cred = credentials.Certificate(credentials_dict)
            elif credentials_path:
                cred = credentials.Certificate(credentials_path)
            else:
                # Priority 2: Environment variables
                env_json = os.getenv("FIREBASE_CREDENTIALS_JSON")
                env_path = os.getenv("FIREBASE_SA_PATH")
                
                if env_json:
                    # Parse JSON string from env
                    try:
                        cred_dict = json.loads(env_json)
                        cred = credentials.Certificate(cred_dict)
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Invalid FIREBASE_CREDENTIALS_JSON: {e}")
                elif env_path:
                    # Use file path from env
                    cred = credentials.Certificate(env_path)
                else:
                    # Priority 3: Application Default Credentials (ADC)
                    cred = credentials.ApplicationDefault()
            
            firebase_admin.initialize_app(cred)
        
        self.db = firestore.client()
    
    def get_proposal_ref(self, network: str, proposal_id: int):
        """Get reference to a proposal document."""
        doc_id = f"{network}-{proposal_id}"
        return self.db.collection('proposals').document(doc_id)
    
    def proposal_exists(self, network: str, proposal_id: int) -> bool:
        """Check if proposal exists."""
        doc = self.get_proposal_ref(network, proposal_id).get()
        return doc.exists
    
    def create_proposal(self, network: str, proposal_id: int, status: str = "discovered"):
        """Create a new proposal document."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        now = firestore.SERVER_TIMESTAMP
        
        doc_ref.set({
            'network': network,
            'proposalId': proposal_id,
            'status': status,
            'discoveredAt': now,
            'createdAt': now,
            'updatedAt': now,
            'createdBy': 'cybergov-scraper',
            'lastModifiedBy': 'cybergov-scraper',
            'version': 1,
            'files': {},
            'votes': []
        })
        
        # Add history entry
        self.add_history_entry(network, proposal_id, 'N/A', status, 'cybergov-scraper')
        
        return doc_ref
    
    def update_proposal_status(self, network: str, proposal_id: int, 
                               new_status: str, modified_by: str = 'cybergov'):
        """Update proposal status."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        doc = doc_ref.get()
        
        if not doc.exists:
            raise ValueError(f"Proposal {network}-{proposal_id} not found")
        
        old_status = doc.to_dict().get('status', 'unknown')
        
        doc_ref.update({
            'status': new_status,
            'updatedAt': firestore.SERVER_TIMESTAMP,
            'lastModifiedBy': modified_by,
            'version': firestore.Increment(1)
        })
        
        self.add_history_entry(network, proposal_id, old_status, new_status, modified_by)
    
    def add_history_entry(self, network: str, proposal_id: int, 
                         from_status: str, to_status: str, changed_by: str):
        """Add entry to proposal history."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        history_ref = doc_ref.collection('history')
        
        history_ref.add({
            'changedAt': firestore.SERVER_TIMESTAMP,
            'changedBy': changed_by,
            'fromStatus': from_status,
            'toStatus': to_status,
            'changes': {
                'status': to_status
            }
        })
    
    def save_file_content(self, network: str, proposal_id: int, 
                         file_type: str, content: Any):
        """Save file content to Firestore."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        
        # For JSON data, store as nested object
        if isinstance(content, dict):
            content_to_store = content
        elif isinstance(content, str):
            content_to_store = content
        else:
            content_to_store = json.dumps(content)
        
        # Store in subcollection for better organization
        files_ref = doc_ref.collection('files').document(file_type)
        files_ref.set({
            'content': content_to_store,
            'uploadedAt': firestore.SERVER_TIMESTAMP,
            'contentType': 'application/json' if isinstance(content, dict) else 'text/markdown'
        })
        
        # Update file reference in main document
        file_path = f"proposals/{network}/{proposal_id}/files/{file_type}"
        doc_ref.update({
            f'files.{file_type}': file_path,
            'updatedAt': firestore.SERVER_TIMESTAMP
        })
        
        return file_path
    
    def get_file_content(self, network: str, proposal_id: int, file_type: str):
        """Retrieve file content from Firestore."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        file_ref = doc_ref.collection('files').document(file_type)
        file_doc = file_ref.get()
        
        if not file_doc.exists:
            raise FileNotFoundError(f"File {file_type} not found for {network}-{proposal_id}")
        
        return file_doc.to_dict().get('content')
    
    def file_exists(self, network: str, proposal_id: int, file_type: str) -> bool:
        """Check if file exists."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        file_ref = doc_ref.collection('files').document(file_type)
        return file_ref.get().exists
    
    def add_error(self, network: str, proposal_id: int, stage: str, 
                  error: str, retry_count: int = 0):
        """Add error to proposal."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        errors_ref = doc_ref.collection('errors')
        
        errors_ref.add({
            'stage': stage,
            'error': error,
            'occurredAt': firestore.SERVER_TIMESTAMP,
            'retryCount': retry_count
        })
    
    def save_vote(self, network: str, proposal_id: int, vote_data: Dict[str, Any], 
                  vote_number: int = 0):
        """Save vote data."""
        doc_ref = self.get_proposal_ref(network, proposal_id)
        
        # Get current votes array
        doc = doc_ref.get()
        votes = doc.to_dict().get('votes', [])
        
        # Update or append vote
        vote_entry = {
            'voteNumber': vote_number,
            'archived': False,
            'finalDecision': vote_data.get('final_decision', ''),
            'txHash': vote_data.get('tx_hash', ''),
            'manifestHash': vote_data.get('manifest_hash', ''),
            'timestamp': vote_data.get('timestamp_utc', ''),
            'isConclusive': vote_data.get('is_conclusive', False),
            'isUnanimous': vote_data.get('is_unanimous', False)
        }
        
        if vote_number < len(votes):
            votes[vote_number] = vote_entry
        else:
            votes.append(vote_entry)
        
        doc_ref.update({
            'votes': votes,
            'updatedAt': firestore.SERVER_TIMESTAMP
        })
        
        # Store full vote details in subcollection
        vote_ref = doc_ref.collection('votes').document(str(vote_number))
        vote_ref.set({
            **vote_data,
            'savedAt': firestore.SERVER_TIMESTAMP
        })

    def create_user(
        self,
        delegatex_user_id: int,
        strategy_id: str = "neutral",
        signature_link: str = None,
        contact_link: str = None,
        wallet_address: str = None,
        preferences: Dict[str, Any] = None,
    ):
        """
        Creates OR updates a DelegateX user.
        Stored at: users/<delegatexUserId>
        """
        user_ref = self.db.collection("users").document(str(delegatex_user_id))

        user_data = {
            "delegatexUserId": delegatex_user_id,
            "strategyId": strategy_id,
            "signature_link": signature_link,
            "contact_link": contact_link,
            "walletAddress": wallet_address,
            "preferences": preferences or {},
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        }

        user_ref.set(user_data, merge=True)
        return user_ref

    def save_user_vote(
        self,
        user_id: str,
        proposal_id: str,
        weighted_result: WeightedDecisionResult,
        proposal_data: Dict[str, Any],
        provenance: Dict[str, Any],
        comment: str,
    ):
        """
        Saves a user's weighted vote to users/{user_id}/votes/{proposal_id}.
        Stores the full vote JSON (including comment) inside vote.content.
        """
        user_ref = self.db.collection("users").document(str(user_id))
        vote_ref = user_ref.collection("votes").document(str(proposal_id))

        # 1. Construct the inner vote content JSON (complex part)
        vote_content = {
            "timestamp_utc": datetime.utcnow().isoformat(),
            "is_conclusive": True,
            "final_decision": weighted_result.final_decision,
            "is_unanimous": False,  # could be calculated if needed
            "summary_rationale": weighted_result.decision_reasoning,
            "votes_breakdown": [
                {
                    "model": v["agent"],
                    "decision": v["vote"],
                    "confidence": None,
                }
                for v in weighted_result.agent_votes
            ],
            "weighted_decision_metadata": {
                "engine_version": weighted_result.engine_version,
                "strategy_used": weighted_result.template_used,
                "template_weights": weighted_result.template_weights,
                "weighted_scores": weighted_result.weighted_scores,
                "margin": weighted_result.margin,
                "confidence": weighted_result.confidence,
                "rules_triggered": weighted_result.rules_triggered,
                "decision_reasoning": weighted_result.decision_reasoning,
                "agent_votes_with_weights": weighted_result.agent_votes,
            },
            # ✅ Include the human-facing comment INSIDE the vote payload
            "comment": comment,
        }

        # 2. Construct the full Firestore vote document
        vote_data = {
            # User reference
            "delegatexUserId": int(user_id),
            "proposal_id": str(proposal_id),
            "voted_at": firestore.SERVER_TIMESTAMP,
            "strategy_used": weighted_result.template_used,
            # Actual vote blob
            "vote": {
                "content": json.dumps(vote_content),
                "hash": "PENDING_CALCULATION",
                "timestamp_utc": datetime.utcnow().isoformat(),
            },
            # Audit trail
            "provenance": {
                "github_run_id": provenance.get("github_run_id"),
                "script": provenance.get("script"),
                "model_name": provenance.get("model_name", "gov-bot-v1"),
                "timestamp": provenance.get(
                    "timestamp",
                    datetime.utcnow().isoformat(),
                ),
            },
            # Snapshot for fast UI loading
            "proposal_snapshot": {
                "title": proposal_data.get("title", "Unknown"),
                "requested_amount": proposal_data.get("requestedAmount")
                or proposal_data.get("onChainInfo", {}).get("usdAmount", "0"),
                "track_number": str(proposal_data.get("trackNumber", "0")),
                "network": proposal_data.get("network", "polkadot"),
                "proposer": proposal_data.get("onChainInfo", {}).get("proposer"),
                "status": proposal_data.get("onChainInfo", {}).get("status"),
            },
        }

        vote_ref.set(vote_data)
        return vote_ref