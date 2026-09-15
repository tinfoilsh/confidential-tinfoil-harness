// Check Go-produced artifacts using the reference webapp's actual validators.
// The JSON on stdin contains an optional base64 ZIP and optional stored rows.
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const { createRequire } = require('node:module')
const root = path.resolve(process.argv[2])
const refRequire = createRequire(path.join(root, 'package.json'))
const ts = refRequire('typescript')
const legacyRoot = path.join(__dirname, 'fixtures/legacy-webapp')
const cache = new Map()
function load(file) {
  if (!path.extname(file)) file += '.ts'
  if (cache.has(file)) return cache.get(file).exports
  const module = { exports: {} }
  cache.set(file, module)
  const js = ts.transpileModule(fs.readFileSync(file, 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText
  const localRequire = name => name.startsWith('.') ? load(path.resolve(path.dirname(file), name)) : name.startsWith('@/') ? load(path.join(legacyRoot, name.slice(2))) : refRequire(name)
  vm.runInThisContext('(function(require,module,exports){' + js + '\n})', { filename: file })(localRequire, module, module.exports)
  return module.exports
}
const input = JSON.parse(fs.readFileSync(0, 'utf8'))
if (input.backup) {
  const { unzipSync } = refRequire('fflate')
  const files = unzipSync(Buffer.from(input.backup, 'base64'))
  const { assertValidNativeBackupV2 } = load(path.join(legacyRoot, 'services/native-backup/format.ts'))
  const manifest = JSON.parse(Buffer.from(files['manifest.json']).toString('utf8'))
  assertValidNativeBackupV2(files['manifest.json'], manifest.files.map(entry => ({ path: entry.path, kind: entry.kind, bytes: files[entry.path] })))
}
if (input.chat || input.profile) {
  const schemas = load(path.join(legacyRoot, 'services/cloud/schemas.ts'))
  if (input.chat) schemas.RemoteChatPlaintextSchema.parse(input.chat)
  if (input.profile) schemas.ProfileDataSchema.parse(input.profile)
}
if (input.events) {
  const { initialChat, reduceEvent } = load(path.join(root, 'src/services/harness/reducer.ts'))
  const assert = require('node:assert/strict')
  let state = initialChat()
  input.events.forEach((event, id) => { state = reduceEvent(state, { event, id }) })
  const fields = ['id', 'type', 'content', 'toolCallId', 'name', 'arguments', 'result', 'progress', 'resolution', 'resolvedAt', 'complete']
  const messages = value => value.map(m => ({ id: m.id, role: m.role, timeline: (m.timeline ?? []).map(b => Object.fromEntries(fields.filter(f => b[f] !== undefined).map(f => [f, b[f]]))) }))
  assert.deepEqual(messages(state.messages), messages(input.expectedMessages))
}
process.stdout.write('Reference compatibility checks passed.\n')
