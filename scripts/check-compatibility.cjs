// Replay Go-produced run events through the web client's harness reducer and
// compare the messages it builds with the ones the harness stored.
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const assert = require('node:assert/strict')
const { createRequire } = require('node:module')
const root = path.resolve(process.argv[2])
const refRequire = createRequire(path.join(root, 'package.json'))
const ts = refRequire('typescript')
const cache = new Map()
function load(file) {
  if (!path.extname(file)) file += '.ts'
  if (cache.has(file)) return cache.get(file).exports
  const module = { exports: {} }
  cache.set(file, module)
  const js = ts.transpileModule(fs.readFileSync(file, 'utf8'), { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.CommonJS } }).outputText
  const localRequire = name => name.startsWith('.') ? load(path.resolve(path.dirname(file), name)) : name.startsWith('@/') ? load(path.join(root, 'src', name.slice(2))) : refRequire(name)
  vm.runInThisContext('(function(require,module,exports){' + js + '\n})', { filename: file })(localRequire, module, module.exports)
  return module.exports
}
const input = JSON.parse(fs.readFileSync(0, 'utf8'))
const { initialChat, reduceEvent } = load(path.join(root, 'src/services/harness/reducer.ts'))
let state = initialChat()
input.events.forEach((event, id) => { state = reduceEvent(state, { event, id }) })
const fields = ['id', 'type', 'content', 'toolCallId', 'name', 'arguments', 'result', 'progress', 'resolution', 'resolvedAt', 'complete']
const messages = value => value.map(m => ({ id: m.id, role: m.role, ...(m.role === 'user' ? { content: m.content } : {}), timeline: (m.timeline ?? []).map(b => Object.fromEntries(fields.filter(f => b[f] !== undefined).map(f => [f, b[f]]))) }))
assert.deepEqual(messages(state.messages), messages(input.expectedMessages))
process.stdout.write('Reference compatibility checks passed.\n')
