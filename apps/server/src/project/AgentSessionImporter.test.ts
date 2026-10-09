import { expect, it } from "@effect/vitest";
import {
  AgentSessionUnavailableError,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";
import * as AgentSessionImporter from "./AgentSessionImporter.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectService from "./ProjectService.ts";

const projectId = ProjectId.make("agent-session-import-project");
const providerInstanceId = ProviderInstanceId.make("codex");
const providerSessionId = "native-codex-thread";
const threadId = ThreadId.make(`import:${providerInstanceId}:${providerSessionId}`);

const claudeInstanceId = ProviderInstanceId.make("claudeAgent");
const claudeSessionId = "123e4567-e89b-42d3-a456-426614174000";
const claudeImportThreadId = ThreadId.make(`import:${claudeInstanceId}:${claudeSessionId}`);

/** Scanner stub with the import path stubs every service field the importer touches. */
function scannerWith(overrides: {
  readonly recentThreads?: AgentSessionScanner.AgentSessionScanner["Service"]["recentThreads"];
  readonly selectedThread?: AgentSessionScanner.AgentSessionScanner["Service"]["selectedThread"];
}): AgentSessionScanner.AgentSessionScanner["Service"] {
  return AgentSessionScanner.AgentSessionScanner.of({
    scan: Effect.die("unused"),
    claudeSessionHomes: Effect.succeed(new Map([[claudeInstanceId, "/home/claude"]])),
    list: () => Effect.die("unused"),
    preview: () => Effect.succeed({ messages: [], nextBefore: null, truncated: false }),
    recentThreads: overrides.recentThreads ?? (() => Stream.empty),
    selectedThread:
      overrides.selectedThread ??
      (() =>
        Effect.succeed({
          _tag: "Importable",
          thread: {
            source: "claudeAgent",
            providerInstanceId: claudeInstanceId,
            providerSessionId: claudeSessionId,
            title: "Selected session",
            model: null,
            createdAt: "2026-09-01T10:00:00.000Z",
            updatedAt: "2026-09-01T10:01:00.000Z",
            messages: [{ role: "user", text: "Hello", createdAt: "2026-09-01T10:00:00.000Z" }],
          },
          source: {
            provider: "claudeAgent",
            providerInstanceId: claudeInstanceId,
            providerSessionId: claudeSessionId,
            filePath: `/home/claude/projects/project/${claudeSessionId}.jsonl`,
            size: 100,
            mtimeMs: 2,
            device: 3,
            inode: 4,
            birthtimeMs: 1,
          },
        })),
  });
}

const projectServiceLayer = Layer.mock(ProjectService.ProjectService)({
  getById: () =>
    Effect.succeed(Option.some({ id: projectId, workspaceRoot: "/workspace/project" } as never)),
});

it.effect("imports messages once and preserves the provider native resume binding", () => {
  const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
  const upserts: Array<unknown> = [];
  const recorded: Array<unknown> = [];
  let imported = false;
  const scanner = scannerWith({
    recentThreads: () =>
      Stream.succeed({
        _tag: "Importable",
        source: {
          provider: "codex",
          providerInstanceId,
          providerSessionId,
          filePath: "/tmp/native-codex-thread.jsonl",
          size: 100,
          mtimeMs: 2,
          device: 3,
          inode: 4,
          birthtimeMs: 1,
        },
        thread: {
          source: "codex",
          providerInstanceId,
          providerSessionId,
          title: "Imported thread",
          model: "gpt-5.4",
          createdAt: "2026-09-01T10:00:00.000Z",
          updatedAt: "2026-09-01T10:01:00.000Z",
          messages: [
            { role: "user", text: "Fix it", createdAt: "2026-09-01T10:00:00.000Z" },
            { role: "assistant", text: "Fixed", createdAt: "2026-09-01T10:01:00.000Z" },
          ],
        },
      }),
  });
  const layerTest = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(AgentSessionScanner.AgentSessionScanner, scanner),
        Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeed(
              Option.some({ id: projectId, workspaceRoot: "/workspace/project" } as never),
            ),
        }),
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: () =>
            imported
              ? Effect.succeed({
                  thread: { id: threadId, projectId, historyOrigin: "v1_import" },
                } as never)
              : Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: (input) =>
            Effect.sync(() => {
              writes.push(input.events);
              imported = true;
              return [];
            }),
        }),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          list: () => Effect.succeed([]),
          upsert: (input) => Effect.sync(() => void upserts.push(input)),
          recordImportedTranscript: (input) => Effect.sync(() => void recorded.push(input)),
        }),
        IdAllocator.layer,
      ),
    ),
  );

  return Effect.gen(function* () {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    expect(yield* importer.importRecentAgentThreads({ projectId })).toEqual({
      importedCount: 1,
      skippedCount: 0,
    });
    expect(yield* importer.importRecentAgentThreads({ projectId })).toEqual({
      importedCount: 1,
      skippedCount: 0,
    });

    expect(writes).toHaveLength(1);
    expect(writes[0]?.map((event) => event.type)).toEqual([
      "thread.created",
      "message.updated",
      "turn-item.updated",
      "message.updated",
      "turn-item.updated",
      "provider-thread.updated",
    ]);
    const created = writes[0]?.find((event) => event.type === "thread.created");
    const providerThread = writes[0]?.find((event) => event.type === "provider-thread.updated");
    expect(created?.payload).toMatchObject({
      id: threadId,
      activeProviderThreadId: providerThread?.payload.id,
      historyOrigin: "v1_import",
    });
    expect(providerThread?.payload).toMatchObject({
      appThreadId: threadId,
      nativeThreadRef: {
        driver: "codex",
        nativeId: providerSessionId,
        strength: "strong",
      },
    });
    expect(
      writes[0]
        ?.filter((event) => event.type === "message.updated")
        .map((event) => event.payload.text),
    ).toEqual(["Fix it", "Fixed"]);
    expect(upserts).toEqual([
      expect.objectContaining({
        threadId,
        providerInstanceId,
        resumeCursor: { threadId: providerSessionId },
      }),
    ]);
    expect(recorded).toHaveLength(2);
  }).pipe(Effect.provide(layerTest));
});

it.effect("attaches a selected Claude session with supervised permissions", () => {
  const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
  let imported = false;
  const layerTest = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(AgentSessionScanner.AgentSessionScanner, scannerWith({})),
        projectServiceLayer,
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: (threadId: ThreadId) =>
            imported && threadId === claudeImportThreadId
              ? Effect.succeed({
                  thread: { id: threadId, projectId, historyOrigin: "v1_import" },
                } as never)
              : Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: (input) =>
            Effect.sync(() => {
              writes.push(input.events);
              imported = true;
              return [];
            }),
        }),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          list: () => Effect.succeed([]),
          upsert: () => Effect.void,
          recordImportedTranscript: () => Effect.void,
        }),
        IdAllocator.layer,
      ),
    ),
  );

  return Effect.gen(function* () {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    const attachInput = {
      projectId,
      expectedWorkspaceRoot: "/workspace/project",
      providerInstanceId: claudeInstanceId,
      providerSessionId: claudeSessionId,
    };
    const first = yield* importer.attach(attachInput);
    expect(first.threadId).toBe(claudeImportThreadId);
    const created = writes[0]?.find((event) => event.type === "thread.created");
    expect(created?.payload).toMatchObject({
      id: claudeImportThreadId,
      runtimeMode: "approval-required",
      historyOrigin: "v1_import",
    });

    // Re-attaching the same session is idempotent and returns the same thread.
    const second = yield* importer.attach(attachInput);
    expect(second.threadId).toBe(claudeImportThreadId);
    expect(writes).toHaveLength(1);
  }).pipe(Effect.provide(layerTest));
});

it.effect("attaching a session a native thread drives opens that thread instead", () => {
  const nativeThreadId = ThreadId.make("native-claude-thread");
  const writes: Array<ReadonlyArray<OrchestrationV2DomainEvent>> = [];
  const layerTest = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(
          AgentSessionScanner.AgentSessionScanner,
          scannerWith({
            selectedThread: () => Effect.die("selectedThread must not run"),
          }),
        ),
        projectServiceLayer,
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: (threadId: ThreadId) =>
            threadId === nativeThreadId
              ? Effect.succeed({
                  thread: { id: nativeThreadId, projectId, historyOrigin: "native" },
                } as never)
              : Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: (input) =>
            Effect.sync(() => {
              writes.push(input.events);
              return [];
            }),
        }),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          list: () =>
            Effect.succeed([
              {
                threadId: nativeThreadId,
                providerName: "claudeAgent",
                providerInstanceId: claudeInstanceId,
                adapterKey: "claudeAgent",
                runtimeMode: "full-access",
                status: "stopped",
                lastSeenAt: "2026-09-01T10:00:00.000Z",
                resumeCursor: { threadId: nativeThreadId, resume: claudeSessionId },
                runtimePayload: null,
              } as never,
            ]),
          upsert: () => Effect.void,
          recordImportedTranscript: () => Effect.void,
        }),
        IdAllocator.layer,
      ),
    ),
  );

  return Effect.gen(function* () {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    const attached = yield* importer.attach({
      projectId,
      expectedWorkspaceRoot: "/workspace/project",
      providerInstanceId: claudeInstanceId,
      providerSessionId: claudeSessionId,
    });
    expect(attached.threadId).toBe(nativeThreadId);
    expect(writes).toHaveLength(0);
  }).pipe(Effect.provide(layerTest));
});

it.effect("attaching a session claimed by another project fails with a structured error", () => {
  const otherProjectId = ProjectId.make("other-project");
  const nativeThreadId = ThreadId.make("foreign-claude-thread");
  const layerTest = AgentSessionImporter.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.succeed(AgentSessionScanner.AgentSessionScanner, scannerWith({})),
        projectServiceLayer,
        Layer.mock(Orchestrator.OrchestratorV2)({
          getThreadRecords: (threadId: ThreadId) =>
            threadId === nativeThreadId
              ? Effect.succeed({
                  thread: {
                    id: nativeThreadId,
                    projectId: otherProjectId,
                    historyOrigin: "native",
                  },
                } as never)
              : Effect.fail(new Orchestrator.OrchestratorProjectionError({ threadId })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          write: () => Effect.succeed([]),
        }),
        Layer.mock(ProviderSessionRuntime.ProviderSessionRuntimeRepository)({
          list: () =>
            Effect.succeed([
              {
                threadId: nativeThreadId,
                providerName: "claudeAgent",
                providerInstanceId: claudeInstanceId,
                adapterKey: "claudeAgent",
                runtimeMode: "full-access",
                status: "stopped",
                lastSeenAt: "2026-09-01T10:00:00.000Z",
                resumeCursor: { threadId: nativeThreadId, resume: claudeSessionId },
                runtimePayload: null,
              } as never,
            ]),
          upsert: () => Effect.void,
          recordImportedTranscript: () => Effect.void,
        }),
        IdAllocator.layer,
      ),
    ),
  );

  return Effect.gen(function* () {
    const importer = yield* AgentSessionImporter.AgentSessionImporter;
    const failure = yield* importer
      .attach({
        projectId,
        expectedWorkspaceRoot: "/workspace/project",
        providerInstanceId: claudeInstanceId,
        providerSessionId: claudeSessionId,
      })
      .pipe(Effect.flip);
    expect(failure).toBeInstanceOf(AgentSessionUnavailableError);
  }).pipe(Effect.provide(layerTest));
});
