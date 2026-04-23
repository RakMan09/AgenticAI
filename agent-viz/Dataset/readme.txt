OpenClaw Dataset Handoff

Overview
This folder contains the real OpenClaw trace dataset collected for the project.

Important context
- The project originally started with synthetic traces to bootstrap the pipeline and UI.
- This handoff is for the real OpenClaw-based dataset.
- Use the raw session trace files as the source of truth.
- Do not rely on older intermediate normalized/probe files unless explicitly needed for debugging.

Files in this handoff
1. <SESSION_1_FILE>.jsonl
   - Main real OpenClaw session trace
   - Contains mixed successful and failed runs
   - Includes user messages, assistant responses, embedded tool calls, tool results, and retry behavior

2. <SESSION_2_FILE>.jsonl
   - Second real OpenClaw session trace
   - Contains fresh-session startup/bootstrap behavior
   - Includes internal read tool calls, successful file reads, missing-file errors, one successful task, and one failed task

3. dataset_summary.txt
   - Human-readable summary of the dataset
   - Includes message/tool counts and important format notes

What is inside the raw trace files
The raw JSONL session files contain event records from OpenClaw sessions.

Common record patterns:
- session / model_change / thinking_level_change / custom
  These are session-level metadata and startup events.

- message.role = "user"
  These are user prompts.

- message.role = "assistant"
  These are assistant responses.
  Important: tool calls are often embedded inside assistant message.content as entries with:
    type = "toolCall"

- message.role = "toolResult"
  These are tool outputs/results.
  Tool success/failure details are often in:
    message.details

Important format note
Tool calls are usually not stored as separate top-level rows.
They are commonly embedded inside assistant messages.

So the most important structures to parse are:
- user messages
- assistant messages
- embedded toolCall objects inside assistant content
- toolResult messages

Typical success pattern
user
-> assistant (contains embedded toolCall)
-> toolResult (success)
-> assistant (final response)

Typical failure pattern
user
-> assistant (contains embedded toolCall)
-> toolResult (error/failure)
-> assistant (error explanation)

Typical retry pattern
user
-> assistant (toolCall)
-> toolResult (failure)
-> assistant or assistant(toolCall again)
-> toolResult (failure/success)
-> assistant (final report)

What to use as the primary dataset
Use the two raw session JSONL files as the primary dataset.

Recommended priority:
1. <SESSION_1_FILE>.jsonl
2. <SESSION_2_FILE>.jsonl

Use dataset_summary.txt for orientation only.
It is not the source of truth.

What not to use
- Do not treat old synthetic traces as the primary dataset for this handoff.
- Do not treat older probe/intermediate normalized files as the main dataset.
- Do not assume a separate top-level toolCall row always exists.

Recommended ingestion approach
1. Read each raw JSONL line by line.
2. Keep all message rows.
3. For assistant messages, inspect message.content for:
   - type = "text"
   - type = "toolCall"
4. For toolResult messages, inspect:
   - message.toolName
   - message.details
   - message.content
5. Build trajectories from event order in the file.

Why this dataset matters
This dataset provides real agent trajectories from OpenClaw, including:
- successful task completions
- tool-use traces
- failures
- retries
- startup/bootstrap behavior
- internal file-read behavior
- missing-file error cases

This is the dataset that should now be used for real-trace analysis in the project.