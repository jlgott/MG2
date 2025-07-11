from CFLOW import Flow, Node
from pydantic import BaseModel, Field
import asyncio
import random
from typing import List, Dict, Optional

class ComplexState(BaseModel):
    # Data processing pipeline state
    raw_data: List[Dict] = []
    processed_data: List[Dict] = []
    validation_errors: List[str] = []
    
    # Flow control
    current_stage: str = "init"
    retry_count: int = 0
    max_retries: int = 3
    
    # Quality control
    quality_score: float = 0.0
    quality_threshold: float = 0.8
    
    # Multi-path routing
    processing_mode: str = "normal"  # normal, fast, thorough
    error_severity: str = "none"     # none, low, medium, high, critical
    
    # Batch processing
    batch_size: int = 10
    current_batch: int = 0
    total_batches: int = 0
    
    # Feature flags & configuration  
    enable_ml_processing: bool = True
    enable_data_enrichment: bool = True
    debug_mode: bool = False
    
    # Results & metrics
    processing_time: float = 0.0
    success_count: int = 0
    error_count: int = 0
    
    # Final routing
    next_action: str = "continue"

# ============ DATA GENERATION & INITIALIZATION ============

def initialize_data(state):
    """Generate initial dataset and configure processing"""
    state.raw_data = [
        {"id": i, "value": random.randint(1, 100), "category": random.choice(["A", "B", "C"])}
        for i in range(50)
    ]
    state.total_batches = len(state.raw_data) // state.batch_size + 1
    state.current_stage = "data_loaded"
    state.processing_mode = random.choice(["normal", "fast", "thorough"])
    print(f"🚀 Initialized with {len(state.raw_data)} records, mode: {state.processing_mode}")
    return state

def validate_input(state):
    """Validate input data quality and set initial routing"""
    errors = []
    for item in state.raw_data:
        if not isinstance(item.get("value"), int):
            errors.append(f"Invalid value type in item {item.get('id')}")
        if item.get("value", 0) < 0:
            errors.append(f"Negative value in item {item.get('id')}")
    
    state.validation_errors = errors
    
    if len(errors) > 10:
        state.error_severity = "critical"
        state.next_action = "abort"
    elif len(errors) > 5:
        state.error_severity = "high" 
        state.next_action = "repair_data"
    elif len(errors) > 0:
        state.error_severity = "low"
        state.next_action = "continue"
    else:
        state.error_severity = "none"
        state.next_action = "continue"
    
    state.current_stage = "validated"
    print(f"📊 Validation: {len(errors)} errors, severity: {state.error_severity}")
    return state

# ============ ERROR HANDLING & REPAIR ============

def repair_data(state):
    """Attempt to repair data issues"""
    repaired = 0
    for item in state.raw_data:
        if item.get("value", 0) < 0:
            item["value"] = abs(item["value"])
            repaired += 1
    
    state.retry_count += 1
    state.current_stage = "repaired"
    
    # Re-route based on repair success
    if repaired > 0:
        state.next_action = "revalidate"
        print(f"🔧 Repaired {repaired} items, retry #{state.retry_count}")
    else:
        state.next_action = "continue"
    
    return state

def handle_critical_error(state):
    """Handle critical errors - cleanup and abort"""
    state.current_stage = "error_cleanup"
    state.processed_data = []
    state.next_action = "abort"
    print("💥 Critical errors detected - aborting pipeline")
    return state

# ============ PROCESSING MODES ============

def normal_processing(state):
    """Standard data processing pipeline"""
    processed = []
    for item in state.raw_data:
        processed_item = {
            "id": item["id"],
            "value": item["value"] * 2,
            "category": item["category"],
            "processed_by": "normal"
        }
        processed.append(processed_item)
    
    state.processed_data = processed
    state.quality_score = random.uniform(0.7, 0.9)
    state.current_stage = "processed_normal"
    state.next_action = "quality_check"
    print(f"⚙️  Normal processing: {len(processed)} items, quality: {state.quality_score:.2f}")
    return state

def fast_processing(state):
    """Fast but lower quality processing"""
    processed = []
    for item in state.raw_data[::2]:  # Skip every other item for speed
        processed_item = {
            "id": item["id"],
            "value": item["value"] * 1.5,
            "category": item["category"],
            "processed_by": "fast"
        }
        processed.append(processed_item)
    
    state.processed_data = processed
    state.quality_score = random.uniform(0.5, 0.7)
    state.current_stage = "processed_fast"
    state.next_action = "quality_check"
    print(f"⚡ Fast processing: {len(processed)} items, quality: {state.quality_score:.2f}")
    return state

async def thorough_processing(state):
    """Thorough processing with ML and enrichment"""
    processed = []
    
    # Simulate ML processing
    if state.enable_ml_processing:
        await asyncio.sleep(0.1)  # Simulate ML inference time
    
    for item in state.raw_data:
        processed_item = {
            "id": item["id"],
            "value": item["value"] * 3,
            "category": item["category"],
            "processed_by": "thorough",
            "ml_score": random.uniform(0.8, 1.0) if state.enable_ml_processing else None
        }
        
        # Data enrichment
        if state.enable_data_enrichment:
            processed_item["enriched_category"] = f"{item['category']}_premium"
        
        processed.append(processed_item)
    
    state.processed_data = processed
    state.quality_score = random.uniform(0.85, 0.98)
    state.current_stage = "processed_thorough"
    state.next_action = "quality_check"
    print(f"🎯 Thorough processing: {len(processed)} items, quality: {state.quality_score:.2f}")
    return state

# ============ QUALITY CONTROL ============

def quality_check(state):
    """Evaluate processing quality and determine next steps"""
    if state.quality_score >= state.quality_threshold:
        state.next_action = "post_process"
        state.success_count = len(state.processed_data)
        print(f"✅ Quality check passed: {state.quality_score:.2f} >= {state.quality_threshold}")
    else:
        state.error_count += 1
        if state.retry_count < state.max_retries:
            state.next_action = "retry_processing"
            print(f"⚠️  Quality check failed, retrying ({state.retry_count}/{state.max_retries})")
        else:
            state.next_action = "fallback_processing"
            print(f"❌ Quality check failed, max retries exceeded")
    
    state.current_stage = "quality_checked"
    return state

def retry_processing(state):
    """Reset for retry with different parameters"""
    state.retry_count += 1
    state.processed_data = []
    
    # Adjust processing mode for retry
    if state.processing_mode == "fast":
        state.processing_mode = "normal"
    elif state.processing_mode == "normal":
        state.processing_mode = "thorough"
    
    state.next_action = "process_data"
    state.current_stage = "retry_setup"
    print(f"🔄 Retry #{state.retry_count}, switching to {state.processing_mode} mode")
    return state

def fallback_processing(state):
    """Last resort processing when quality checks fail"""
    # Simple fallback - just copy raw data
    state.processed_data = [{"id": item["id"], "value": item["value"], "fallback": True} 
                           for item in state.raw_data]
    state.quality_score = 0.6  # Fixed lower quality
    state.next_action = "post_process"
    state.current_stage = "fallback_complete"
    print("🆘 Fallback processing applied")
    return state

# ============ POST PROCESSING ============

def post_process(state):
    """Final data transformations and routing"""
    # Add metadata to all processed items
    for item in state.processed_data:
        item["processing_timestamp"] = "2025-01-01T00:00:00Z"
        item["pipeline_version"] = "v2.0"
    
    # Determine final routing based on multiple factors
    if state.debug_mode:
        state.next_action = "debug_output"
    elif state.quality_score >= 0.95:
        state.next_action = "premium_output"
    elif len(state.processed_data) > 40:
        state.next_action = "batch_output"
    else:
        state.next_action = "standard_output"
    
    state.current_stage = "post_processed"
    print(f"🎁 Post-processing complete, routing to: {state.next_action}")
    return state

# ============ OUTPUT HANDLERS ============

def debug_output(state):
    """Debug output with detailed information"""
    print("🐛 DEBUG OUTPUT:")
    print(f"   Processed: {len(state.processed_data)} items")
    print(f"   Quality: {state.quality_score:.3f}")
    print(f"   Retries: {state.retry_count}")
    print(f"   Errors: {len(state.validation_errors)}")
    state.current_stage = "debug_complete"
    state.next_action = "finish"
    return state

def premium_output(state):
    """High-quality output processing"""
    print(f"💎 PREMIUM OUTPUT: {len(state.processed_data)} high-quality items")
    state.current_stage = "premium_complete"
    state.next_action = "finish"
    return state

def batch_output(state):
    """Batch output for large datasets"""
    print(f"📦 BATCH OUTPUT: {len(state.processed_data)} items in {state.total_batches} batches")
    state.current_stage = "batch_complete"
    state.next_action = "finish"
    return state

def standard_output(state):
    """Standard output processing"""
    print(f"📋 STANDARD OUTPUT: {len(state.processed_data)} items processed")
    state.current_stage = "standard_complete"
    state.next_action = "finish"
    return state

def pipeline_complete(state):
    """Final pipeline completion"""
    print("🏁 PIPELINE COMPLETE!")
    print(f"   Final stage: {state.current_stage}")
    print(f"   Success rate: {state.success_count}/{state.success_count + state.error_count}")
    print(f"   Final quality: {state.quality_score:.2f}")
    return state

def abort_pipeline(state):
    """Abort the entire pipeline"""
    print("🛑 PIPELINE ABORTED!")
    print(f"   Reason: {state.error_severity} errors detected")
    print(f"   Errors: {len(state.validation_errors)}")
    return state

# ============ COMPLEX GRAPH ASSEMBLY ============

async def main():
    print("🔧 Building complex multi-stage data processing pipeline...")
    
    # Create all nodes
    init_node = Node(initialize_data)
    validate_node = Node(validate_input)
    repair_node = Node(repair_data)
    critical_error_node = Node(handle_critical_error)
    
    # Processing mode nodes
    normal_proc_node = Node(normal_processing)
    fast_proc_node = Node(fast_processing) 
    thorough_proc_node = Node(thorough_processing)
    
    # Quality control nodes
    quality_node = Node(quality_check)
    retry_node = Node(retry_processing)
    fallback_node = Node(fallback_processing)
    
    # Post processing
    post_proc_node = Node(post_process)
    
    # Output nodes
    debug_node = Node(debug_output)
    premium_node = Node(premium_output)
    batch_node = Node(batch_output)
    standard_node = Node(standard_output)
    
    # Terminal nodes
    complete_node = Node(pipeline_complete)
    abort_node = Node(abort_pipeline)
    
    # ============ COMPLEX GRAPH DEFINITION ============
    
    # Initial flow
    init_node >> validate_node
    
    # Validation routing
    validate_node - ("next_action", "continue") >> normal_proc_node  # Default to normal
    validate_node - ("next_action", "repair_data") >> repair_node
    validate_node - ("next_action", "abort") >> critical_error_node
    
    # Error handling
    repair_node - ("next_action", "revalidate") >> validate_node  # Loop back
    repair_node - ("next_action", "continue") >> normal_proc_node
    critical_error_node >> abort_node
    
    # Processing mode routing (based on processing_mode set during init)
    # We'll route from normal_proc_node based on processing_mode
    normal_proc_node - ("processing_mode", "normal") >> quality_node
    normal_proc_node - ("processing_mode", "fast") >> fast_proc_node
    normal_proc_node - ("processing_mode", "thorough") >> thorough_proc_node
    
    # All processing modes lead to quality check
    fast_proc_node >> quality_node
    thorough_proc_node >> quality_node
    
    # Quality control routing
    quality_node - ("next_action", "post_process") >> post_proc_node
    quality_node - ("next_action", "retry_processing") >> retry_node
    quality_node - ("next_action", "fallback_processing") >> fallback_node
    
    # Retry and fallback
    retry_node - ("next_action", "process_data") >> normal_proc_node  # Loop back
    fallback_node >> post_proc_node
    
    # Post processing routing
    post_proc_node - ("next_action", "debug_output") >> debug_node
    post_proc_node - ("next_action", "premium_output") >> premium_node
    post_proc_node - ("next_action", "batch_output") >> batch_node
    post_proc_node - ("next_action", "standard_output") >> standard_node
    
    # All outputs lead to completion
    debug_node >> complete_node
    premium_node >> complete_node
    batch_node >> complete_node
    standard_node >> complete_node
    
    # ============ EXECUTION ============
    
    # Add printing to all nodes for visibility
    def create_print_post(node):
        def print_post(state, result):
            print(f"  └─ Node '{node.id}' → Stage: {result.current_stage}, Next: {result.next_action}")
            return result
        return print_post
    
    all_nodes = [
        init_node, validate_node, repair_node, critical_error_node,
        normal_proc_node, fast_proc_node, thorough_proc_node,
        quality_node, retry_node, fallback_node, post_proc_node,
        debug_node, premium_node, batch_node, standard_node,
        complete_node, abort_node
    ]
    
    for node in all_nodes:
        node.post = create_print_post(node)
    
    # Create flow and execute
    flow = Flow()
    flow.set_start(init_node)
    
    # Test with different configurations
    test_configs = [
        {"debug_mode": True, "quality_threshold": 0.5},
        {"debug_mode": False, "quality_threshold": 0.95, "enable_ml_processing": True},
        {"debug_mode": False, "quality_threshold": 0.8, "enable_data_enrichment": False}
    ]
    
    for i, config in enumerate(test_configs):
        print(f"\n{'='*60}")
        print(f"🧪 TEST RUN #{i+1}: {config}")
        print(f"{'='*60}")
        
        initial_state = ComplexState(**config)
        result = await flow.run(initial_state)
        
        print(f"\n✨ Test #{i+1} completed: {result.current_stage}")
        print("-" * 60)

if __name__ == "__main__":
    asyncio.run(main())