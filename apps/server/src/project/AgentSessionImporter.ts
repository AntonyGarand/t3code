import {
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  AgentSessionAttachInput,
  AgentSessionAttachResult,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionImportSource,
  AgentSessionListInput,
  AgentSessionListResult,
  AgentSessionPreviewInput,
  AgentSessionPreviewResult,
  AgentSessionScanError,
  AgentSessionSelection,
  AgentSessionSource,
  AgentSessionUnavailableError,
  CLAUDE_SESSION_ID_PATTERN,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type OrchestrationV2AppThread,
  type OrchestrationV2ConversationMessage,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
  type RuntimeMode,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const IMPORT_EVENT_PREFIX = "agent-session-import:v2";
const decodeImportedTranscriptPayload = Schema.decodeUnknownOption(
  Schema.Struct({
    cwd: Schema.optional(Schema.String),
    importedTranscripts: Schema.optional(Schema.Array(AgentSessionImportSource)),
  }),
);
const decodeClaudeResumeCursor = Schema.decodeUnknownOption(
  Schema.Struct({ resume: Schema.String }),
);

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' already contains non-imported activity.`;
  }
}

function dateTime(value: string): DateTime.Utc {
  return DateTime.makeUnsafe(value);
}

function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly index: number;
  readonly message: AgentSessionScanner.AgentSessionThreadMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const ordinal = input.index + 1;
  const suffix = String(input.index).padStart(6, "0");
  const messageId = MessageId.make(`${input.threadId}:${suffix}`);
  const turnItemId = TurnItemId.make(
    `${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`,
  );
  const at = dateTime(input.message.createdAt);
  const message: OrchestrationV2ConversationMessage = {
    createdBy: input.message.role === "user" ? "user" : "agent",
    creationSource: "server",
    id: messageId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    role: input.message.role,
    text: input.message.text,
    attachments: [],
    streaming: false,
    createdAt: at,
    updatedAt: at,
  };
  const common = {
    id: turnItemId,
    threadId: input.threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  const turnItem: OrchestrationV2TurnItem =
    input.message.role === "user"
      ? {
          ...common,
          createdBy: "user",
          creationSource: "server",
          type: "user_message",
          messageId,
          inputIntent: "turn_start",
          text: input.message.text,
          attachments: [],
        }
      : {
          ...common,
          type: "assistant_message",
          messageId,
          text: input.message.text,
          streaming: false,
        };
  return [
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:message:${input.threadId}:${suffix}`),
      type: "message.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: message,
    },
    {
      id: EventId.make(`${IMPORT_EVENT_PREFIX}:turn-item:${input.threadId}:${suffix}`),
      type: "turn-item.updated",
      threadId: input.threadId,
      occurredAt: at,
      payload: turnItem,
    },
  ];
}

interface ImportableAgentSessionThread {
  readonly thread: AgentSessionScanner.AgentSessionThread;
  readonly source: AgentSessionImportSource;
}

interface ProjectScope {
  readonly projectId: ProjectId;
  readonly workspaceRoot: string;
}

const importedThreadId = (selection: AgentSessionSelection) =>
  ThreadId.make(`import:${selection.providerInstanceId}:${selection.providerSessionId}`);

const isAgentSessionUnavailableError = Schema.is(AgentSessionUnavailableError);

const make = Effect.gen(function* () {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectService.ProjectService;
  const eventSink = yield* EventSink.EventSinkV2;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;

  // Bulk imports and single-session attachments exclude each other before
  // resolving an existing thread, so two concurrent callers cannot create the
  // same import thread twice through different home aliases.
  const importLock = yield* Semaphore.make(1);

  const resolveProject = Effect.fn("AgentSessionImporter.resolveProject")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
          onSome: Effect.succeed,
        }),
      ),
    );
    if (
      input.expectedWorkspaceRoot !== undefined &&
      normalizeProjectPathForComparison(project.workspaceRoot) !==
        normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
    ) {
      return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
    }
    return project;
  });

  /**
   * Find a thread that already drives the selected Claude session, whether it
   * is a native thread (its runtime resume cursor names the session) or a
   * previously imported one. Aliases that share a Claude configuration
   * directory are keyed by home identity, not instance id.
   */
  const existingSessionThread = Effect.fn("AgentSessionImporter.existingSessionThread")(function* (
    projectId: ProjectId,
    selection: AgentSessionSelection,
  ) {
    const homes = yield* scanner.claudeSessionHomes;
    const sessionKey = (instanceId: ProviderInstanceId | null, sessionId: string) => {
      const home = instanceId === null ? undefined : homes.get(instanceId);
      return `${home === undefined ? `instance:${instanceId}` : `home:${home}`}\0${sessionId}`;
    };
    const wanted = sessionKey(selection.providerInstanceId, selection.providerSessionId);
    const rows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const candidates = rows.flatMap((row) => {
      if (row.providerName !== "claudeAgent") return [];
      // Import-namespaced threads are resolved directly by thread id in
      // `attach`; only native sessions matter here.
      if (row.threadId.startsWith("import:")) return [];
      const cursor = decodeClaudeResumeCursor(row.resumeCursor);
      if (Option.isNone(cursor)) return [];
      return sessionKey(row.providerInstanceId, cursor.value.resume) === wanted ? [row] : [];
    });
    let fallback: {
      readonly threadId: ThreadId;
      readonly projectId: ProjectId;
    } | null = null;
    for (const candidate of candidates) {
      const records = yield* Effect.option(orchestrator.getThreadRecords(candidate.threadId, []));
      if (Option.isNone(records)) continue;
      const owner = records.value.thread;
      if (owner.projectId === projectId) {
        return { threadId: candidate.threadId, projectId: owner.projectId };
      }
      if (fallback === null)
        fallback = { threadId: candidate.threadId, projectId: owner.projectId };
    }
    return fallback;
  });

  const importAgentThread = Effect.fn("AgentSessionImporter.importAgentThread")(function* (
    scope: ProjectScope,
    outcome: ImportableAgentSessionThread,
    runtimeMode: RuntimeMode,
  ) {
    const thread = outcome.thread;
    const source = outcome.source;
    if (
      thread.source === "claudeAgent" &&
      !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
    ) {
      return yield* new AgentSessionUnresumableSessionError({
        source: thread.source,
        providerSessionId: thread.providerSessionId,
      });
    }
    const threadId = ThreadId.make(
      `import:${source.providerInstanceId}:${source.providerSessionId}`,
    );
    const existing = yield* Effect.option(orchestrator.getThreadRecords(threadId, []));
    if (Option.isSome(existing)) {
      if (existing.value.thread.projectId !== scope.projectId) {
        return yield* new AgentSessionThreadProjectConflictError({
          threadId,
          expectedProjectId: scope.projectId,
          actualProjectId: existing.value.thread.projectId,
        });
      }
      if (existing.value.thread.historyOrigin !== "v1_import") {
        return yield* new AgentSessionThreadModifiedError({ threadId });
      }
      yield* runtimes.recordImportedTranscript({ threadId, source }).pipe(Effect.ignore);
      return threadId;
    }

    const driver = ProviderDriverKind.make(thread.source);
    const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL;
    const providerThreadId = idAllocator.derive.providerThread({
      driver,
      nativeThreadId: thread.providerSessionId,
    });
    const createdAt = dateTime(thread.createdAt);
    const updatedAt = dateTime(thread.updatedAt);
    const appThread: OrchestrationV2AppThread = {
      createdBy: "system",
      creationSource: "server",
      id: threadId,
      projectId: scope.projectId,
      title: thread.title.trim() === "" ? "Untitled thread" : thread.title,
      providerInstanceId: thread.providerInstanceId,
      modelSelection: { instanceId: thread.providerInstanceId, model },
      runtimeMode,
      interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
      branch: null,
      worktreePath: null,
      linkedPullRequest: null,
      branchPullRequest: null,
      activeProviderThreadId: providerThreadId,
      historyOrigin: "v1_import",
      lineage: {
        parentThreadId: null,
        relationshipToParent: null,
        rootThreadId: threadId,
      },
      forkedFrom: null,
      createdAt,
      updatedAt,
      archivedAt: null,
      settledOverride: "settled",
      settledAt: updatedAt,
      unsettledAt: null,
      snoozedUntil: null,
      snoozedAt: null,
      pinnedAt: null,
      pinOrderKey: null,
      activeOrderKey: null,
      lastVisitedAt: null,
      deletedAt: null,
    };
    const providerThread: OrchestrationV2ProviderThread = {
      id: providerThreadId,
      driver,
      providerInstanceId: thread.providerInstanceId,
      providerSessionId: null,
      appThreadId: threadId,
      ownerNodeId: null,
      nativeThreadRef: {
        driver,
        nativeId: thread.providerSessionId,
        strength: "strong",
      },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      createdAt,
      updatedAt,
    };

    yield* runtimes.upsert(
      {
        threadId,
        providerName: driver,
        providerInstanceId: thread.providerInstanceId,
        adapterKey: driver,
        runtimeMode,
        status: "stopped",
        lastSeenAt: thread.updatedAt,
        resumeCursor:
          thread.source === "codex"
            ? { threadId: thread.providerSessionId }
            : { threadId, resume: thread.providerSessionId },
        runtimePayload: { cwd: scope.workspaceRoot },
      },
      { onConflict: "ignore" },
    );
    yield* eventSink.write({
      events: [
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:thread:${threadId}:created`),
          type: "thread.created",
          threadId,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: createdAt,
          payload: appThread,
        },
        ...thread.messages.flatMap((message, index) => messageEvents({ threadId, index, message })),
        {
          id: EventId.make(`${IMPORT_EVENT_PREFIX}:provider-thread:${providerThreadId}`),
          type: "provider-thread.updated",
          threadId,
          driver,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: updatedAt,
          payload: providerThread,
        },
      ],
    });
    yield* runtimes.recordImportedTranscript({ threadId, source });
    return threadId;
  });

  const importRecentAgentThreads = Effect.fn("importRecentAgentThreadsV2")(function* (
    input: AgentSessionImportInput,
  ) {
    const project = yield* resolveProject(input);
    const runtimeRows = yield* runtimes
      .list()
      .pipe(
        Effect.mapError(
          (cause) => new AgentSessionScanError({ operation: "read-projects", cause }),
        ),
      );
    const completedSources = runtimeRows.flatMap((runtime) => {
      const payload = decodeImportedTranscriptPayload(runtime.runtimePayload);
      if (
        Option.isNone(payload) ||
        payload.value.cwd === undefined ||
        normalizeProjectPathForComparison(payload.value.cwd) !==
          normalizeProjectPathForComparison(project.workspaceRoot)
      ) {
        return [];
      }
      return payload.value.importedTranscripts ?? [];
    });
    const outcomes = scanner.recentThreads(project.workspaceRoot, completedSources);
    const importedThreadIds = new Set<ThreadId>();
    let importedCount = 0;
    let skippedCount = 0;

    yield* Stream.runForEach(outcomes, (outcome) =>
      Effect.gen(function* () {
        if (outcome._tag === "Skipped") {
          skippedCount += 1;
          return;
        }
        const threadId = ThreadId.make(
          `import:${outcome.source.providerInstanceId}:${outcome.source.providerSessionId}`,
        );
        if (outcome._tag === "AlreadyImported") {
          importedThreadIds.add(threadId);
          importedCount += 1;
          return;
        }
        if (outcome._tag === "Duplicate") {
          if (importedThreadIds.has(threadId)) {
            yield* runtimes
              .recordImportedTranscript({ threadId, source: outcome.source })
              .pipe(Effect.ignore);
          }
          return;
        }

        const imported = yield* importAgentThread(
          { projectId: input.projectId, workspaceRoot: project.workspaceRoot },
          outcome,
          DEFAULT_RUNTIME_MODE,
        ).pipe(
          Effect.catch((cause) =>
            Effect.logWarning("Could not import an agent session", {
              provider: outcome.thread.source,
              sessionId: outcome.thread.providerSessionId,
              cause,
            }).pipe(Effect.as(null)),
          ),
        );
        if (imported !== null) {
          importedThreadIds.add(imported);
          importedCount += 1;
        } else {
          skippedCount += 1;
        }
      }),
    );

    return { importedCount, skippedCount } satisfies AgentSessionImportResult;
  }, importLock.withPermits(1));

  const list = Effect.fn("AgentSessionImporter.list")(function* (input: AgentSessionListInput) {
    const project = yield* resolveProject(input);
    const result = yield* scanner.list(project.workspaceRoot, input.cursor);
    const existing = yield* Effect.forEach(
      result.sessions,
      (session) =>
        Effect.map(existingSessionThread(input.projectId, session), (match) => ({
          session,
          match,
        })),
      { concurrency: "unbounded" },
    );
    return {
      ...result,
      sessions: existing.map(({ session, match }) => ({
        ...session,
        existingThreadId:
          match !== null && match.projectId === input.projectId ? match.threadId : null,
      })),
    };
  });

  const preview = Effect.fn("AgentSessionImporter.preview")(function* (
    input: AgentSessionPreviewInput,
  ) {
    const project = yield* resolveProject(input);
    return yield* scanner.preview(project.workspaceRoot, input, input.before);
  });

  const attach = Effect.fn("AgentSessionImporter.attach")(function* (
    input: AgentSessionAttachInput,
  ) {
    const project = yield* resolveProject(input);
    const selection: AgentSessionSelection = {
      providerInstanceId: input.providerInstanceId,
      providerSessionId: input.providerSessionId,
    };

    // A previous import of the same session is idempotent when it still owns
    // the thread, and conflicts when another project claimed it.
    const importId = importedThreadId(selection);
    const importedExisting = yield* Effect.option(orchestrator.getThreadRecords(importId, []));
    if (Option.isSome(importedExisting)) {
      if (importedExisting.value.thread.projectId !== input.projectId) {
        return yield* new AgentSessionUnavailableError({
          message:
            "This session is already attached to another project. Open it from that project instead.",
        });
      }
      if (importedExisting.value.thread.historyOrigin === "v1_import") {
        yield* scanner.preview(project.workspaceRoot, selection);
        return { threadId: importId };
      }
    }

    const native = yield* existingSessionThread(input.projectId, selection);
    if (native !== null) {
      if (native.projectId !== input.projectId) {
        return yield* new AgentSessionUnavailableError({
          message:
            "This session is already attached to another project. Open it from that project instead.",
        });
      }
      yield* scanner.preview(project.workspaceRoot, selection);
      return { threadId: native.threadId };
    }

    const discovered = yield* scanner.selectedThread(project.workspaceRoot, selection);
    const threadId = yield* importAgentThread(
      { projectId: input.projectId, workspaceRoot: project.workspaceRoot },
      discovered,
      "approval-required",
    ).pipe(
      Effect.catch((cause) =>
        isAgentSessionUnavailableError(cause)
          ? Effect.fail(cause)
          : Effect.fail(
              new AgentSessionUnavailableError({
                message: "The session could not be attached. Refresh and try again.",
              }),
            ),
      ),
    );
    return { threadId };
  }, importLock.withPermits(1));

  return { importRecentAgentThreads, list, preview, attach };
});

type AgentSessionImporterShape = Effect.Success<typeof make>;

export class AgentSessionImporter extends Context.Service<
  AgentSessionImporter,
  AgentSessionImporterShape
>()("t3/project/AgentSessionImporter") {}

export const layer = Layer.effect(AgentSessionImporter, make);
