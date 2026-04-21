<?xml version="1.0" encoding="UTF-8"?>
<mxfile host="app.diagrams.net" modified="2026-04-21T10:15:00.000Z" agent="Codex" version="24.7.17" compressed="false">
  <diagram id="asio-yield-flow" name="Asio Yield Flow">
    <mxGraphModel dx="1320" dy="820" grid="1" gridSize="10" guides="1" tooltips="1" connect="1" arrows="1" fold="1" page="1" pageScale="1" pageWidth="1100" pageHeight="1300" math="0" shadow="0">
      <root>
        <mxCell id="0" />
        <mxCell id="1" parent="0" />
        <mxCell id="title" value="Boost.Asio 与 yield_t 的交互工作流程" style="text;html=1;strokeColor=none;fillColor=none;align=center;verticalAlign=middle;fontSize=24;fontStyle=1;fontColor=#222222;" vertex="1" parent="1">
          <mxGeometry x="160" y="24" width="760" height="34" as="geometry" />
        </mxCell>
        <mxCell id="subtitle" value="同一工作线程中的 Fiber 通过 completion token 与 Asio 异步操作桥接，在 I/O 就绪后恢复原协程上下文" style="text;html=1;strokeColor=none;fillColor=none;align=center;verticalAlign=middle;fontSize=13;fontColor=#666666;" vertex="1" parent="1">
          <mxGeometry x="150" y="64" width="780" height="26" as="geometry" />
        </mxCell>
        <mxCell id="lane1" value="Fiber" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="135" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane2" value="桥接层" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="231" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane3" value="Asio" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="327" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane4" value="EventLoop" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="423" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane5" value="Kernel" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="519" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane6" value="Asio" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="615" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane7" value="桥接层" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="711" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="lane8" value="Fiber" style="text;html=1;strokeColor=none;fillColor=none;align=right;verticalAlign=middle;fontSize=16;fontStyle=1;fontColor=#5A5A5A;" vertex="1" parent="1">
          <mxGeometry x="40" y="807" width="110" height="32" as="geometry" />
        </mxCell>
        <mxCell id="n1" value="&lt;b&gt;1&lt;/b&gt; 连接 Fiber 执行命令逻辑，并调用&lt;br&gt;&lt;font style=&quot;font-family: monospace;&quot;&gt;async_read(..., yield_t)&lt;/font&gt;" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#FBECEE;strokeColor=#A81D34;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="116" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n2" value="&lt;b&gt;2&lt;/b&gt; completion token 桥接层扩展&lt;br&gt;&lt;font style=&quot;font-family: monospace;&quot;&gt;yield_t&lt;/font&gt; 与 &lt;font style=&quot;font-family: monospace;&quot;&gt;async_result&lt;/font&gt;，保存当前 Fiber 的恢复点" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#EEF5F1;strokeColor=#457E66;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="212" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n3" value="&lt;b&gt;3&lt;/b&gt; &lt;font style=&quot;font-family: monospace;&quot;&gt;io_context&lt;/font&gt; 为 socket 注册异步读写操作，&lt;br&gt;并将完成处理器关联到事件循环" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#EEF3FA;strokeColor=#3F5E8F;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="308" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n4" value="&lt;b&gt;4&lt;/b&gt; 当前 Fiber 挂起，工作线程返回事件循环，&lt;br&gt;继续调度其他 ready Fiber" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#EEF3FA;strokeColor=#3F5E8F;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="404" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n5" value="&lt;b&gt;5&lt;/b&gt; socket 变为可读或可写，内核将 I/O 完成事件&lt;br&gt;返回给事件循环" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#FCF7E8;strokeColor=#AD8837;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="500" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n6" value="&lt;b&gt;6&lt;/b&gt; Asio 在同一工作线程中执行完成回调，&lt;br&gt;并把结果交回 completion token" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#EEF3FA;strokeColor=#3F5E8F;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="596" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n7" value="&lt;b&gt;7&lt;/b&gt; 桥接层唤醒原 Fiber，回填返回值或错误码，&lt;br&gt;恢复挂起的执行上下文" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#EEF5F1;strokeColor=#457E66;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="692" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="n8" value="&lt;b&gt;8&lt;/b&gt; 原 Fiber 从挂起点继续执行，随后进入&lt;br&gt;请求解析、命令执行或响应发送路径" style="rounded=1;whiteSpace=wrap;html=1;arcSize=10;fillColor=#FBECEE;strokeColor=#A81D34;strokeWidth=2;fontColor=#222222;fontSize=15;align=left;verticalAlign=middle;spacing=12;" vertex="1" parent="1">
          <mxGeometry x="180" y="788" width="760" height="72" as="geometry" />
        </mxCell>
        <mxCell id="e1" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n1" target="n2">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="e2" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n2" target="n3">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="e3" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n3" target="n4">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="e4" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n4" target="n5">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="e5" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n5" target="n6">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="e6" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n6" target="n7">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
        <mxCell id="e7" style="edgeStyle=orthogonalEdgeStyle;rounded=0;orthogonalLoop=1;jettySize=auto;html=1;strokeColor=#666666;strokeWidth=2;endArrow=block;endFill=1;" edge="1" parent="1" source="n7" target="n8">
          <mxGeometry relative="1" as="geometry" />
        </mxCell>
      </root>
    </mxGraphModel>
  </diagram>
</mxfile>
