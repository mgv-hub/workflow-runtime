import { WorkflowDefinitionError } from '../errors.js';
import type { WorkflowDef } from './types.js';

export function validateWorkflow(def: WorkflowDef): string[] {
  if (!def.id) throw new WorkflowDefinitionError('Workflow definition requires an id');
  if (!Array.isArray(def.steps))
    throw new WorkflowDefinitionError(`Workflow "${def.id}" requires a steps array`, def.id);


  const seen = new Set<string>();

  for (const step of def.steps) {
    if (!step.id) throw new WorkflowDefinitionError('Every step requires an id', def.id);
    if (seen.has(step.id))
      throw new WorkflowDefinitionError(`Duplicate step id "${step.id}"`, def.id);
    seen.add(step.id);
  }

  for (const step of def.steps) {
    for (const dep of step.dependsOn ?? []) {
      if (!seen.has(dep)) {
        throw new WorkflowDefinitionError(
          `Step "${step.id}" depends on unknown step "${dep}"`,
          def.id,
        );
      }
    }
  }

  // Kahn cycle check.
  const indegree = new Map<string, number>();
  const dependents = new Map<string, string[]>();

  for (const step of def.steps) {
    indegree.set(step.id, (step.dependsOn ?? []).length);

    for (const dep of step.dependsOn ?? []) {
      const list = dependents.get(dep) ?? [];
      list.push(step.id);
      dependents.set(dep, list);
    }
  }

  const queue = def.steps.filter((s) => (s.dependsOn ?? []).length === 0).map((s) => s.id);
  const order: string[] = [];

  while (queue.length > 0) {
    const id = queue.shift()!;
    order.push(id);
    for (const next of dependents.get(id) ?? []) {
      const d = indegree.get(next)! - 1;
      indegree.set(next, d);
      if (d === 0) queue.push(next);
    }
  }

  if (order.length !== def.steps.length) {
    throw new WorkflowDefinitionError(`Workflow "${def.id}" contains a dependency cycle`, def.id);
  }
  return order;
}

export class WorkflowRegistry {
  private byId = new Map<string, Map<string, WorkflowDef>>();

  register(def: WorkflowDef): WorkflowDef {
    validateWorkflow(def);

    const version = String(def.version ?? 1);
    let versions = this.byId.get(def.id);
    if (!versions) this.byId.set(def.id, (versions = new Map()));

    if (versions.has(version)) {
      // Re-registering the same version replaces it; running executions keep
      // their own captured definition snapshot.
      versions.delete(version);
    }
  
    versions.set(version, def);
    return def;
  }

  get(id: string, version?: number | string): WorkflowDef | undefined {
    const versions = this.byId.get(id);
    if (!versions) return undefined;
    if (version != null) return versions.get(String(version));
    return this.latest(id);
  }

  latest(id: string): WorkflowDef | undefined {
    const versions = this.byId.get(id);
    if (!versions || versions.size === 0) return undefined;
    let best: WorkflowDef | undefined;
    for (const def of versions.values()) {
      const v = Number(def.version ?? 1);
      if (!best || v > Number(best.version ?? 1)) best = def;
    }
    return best;
  }

  list(): WorkflowDef[] {
    const out: WorkflowDef[] = [];
    for (const versions of this.byId.values()) out.push(...versions.values());
    return out;
  }
}
