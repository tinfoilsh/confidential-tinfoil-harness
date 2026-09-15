// Regenerate the server declarations from a checked-out webapp.
// Usage: node scripts/port-widgets.cjs ../tinfoil-webapp
const fs = require('node:fs')
const path = require('node:path')
const vm = require('node:vm')
const { createRequire } = require('node:module')
const root = path.resolve(process.argv[2])
const refRequire = createRequire(path.join(root, 'package.json'))
const ts = refRequire('typescript')
const { z } = refRequire('zod')
const { zodToJsonSchema } = refRequire('zod-to-json-schema')
const order = ['StatCards', 'Timeline', 'Chart', 'Image', 'LinkPreview', 'ArtifactPreview', 'Clock', 'RecipeCard', 'MessageCompose', 'SportsData', 'Map']
const widgets = order.map(name => {
  const file = path.join(root, 'src/components/chat/genui/widgets', name + '.tsx')
  const source = fs.readFileSync(file, 'utf8')
  const ast = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
  const declarations = new Map()
  for (const statement of ast.statements) {
    if (ts.isVariableStatement(statement)) for (const decl of statement.declarationList.declarations) declarations.set(decl.name.getText(ast), decl)
  }
  const widget = declarations.get('widget').initializer.arguments[0]
  const props = Object.fromEntries(widget.properties.filter(ts.isPropertyAssignment).map(p => [p.name.getText(ast), p.initializer]))
  const selected = new Set()
  function collect(node) {
    if (ts.isIdentifier(node) && declarations.has(node.text) && !selected.has(node.text)) {
      selected.add(node.text)
      collect(declarations.get(node.text).initializer)
    }
    ts.forEachChild(node, collect)
  }
  collect(declarations.get('schema').initializer)
  selected.add('schema')
  const code = [...declarations].filter(([key]) => selected.has(key)).map(([, value]) => 'const ' + value.getText(ast)).join('\n')
  const fields = ['name', 'description', 'promptHint', 'surface'].filter(k => props[k]).map(k => JSON.stringify(k) + ':' + props[k].getText(ast))
  const js = ts.transpileModule(code + '\nresult = {schema,' + fields.join(',') + '}', { compilerOptions: { target: ts.ScriptTarget.ES2020 }}).outputText
  const sandbox = { z, result: null }
  vm.runInNewContext(js, sandbox, { timeout: 1000 })
  const result = sandbox.result
  result.schema = zodToJsonSchema(result.schema, { target: 'openApi3', $refStrategy: 'none' })
  result.surface ??= 'inline'
  return result
})
// Compare renderer validation with the server-owned tool declarations.
const assert = require('node:assert/strict')
const canonical = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'widgets.json'), 'utf8'))
function validationOnly(value) {
  if (Array.isArray(value)) return value.map(validationOnly)
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).filter(([key]) => key !== 'description').map(([key, child]) => [key, validationOnly(child)]))
  return value
}
for (const widget of widgets) {
  const declaration = canonical.find(entry => entry.name === widget.name)
  assert.ok(declaration, 'Missing server declaration for ' + widget.name)
  assert.deepEqual(validationOnly(JSON.parse(JSON.stringify(widget.schema))), validationOnly(declaration.schema), widget.name + ' schema differs')
  assert.equal(widget.surface, declaration.surface ?? 'inline')
}
assert.equal(widgets.length, canonical.length)
process.stdout.write('Widget renderer contracts match the harness.\n')
