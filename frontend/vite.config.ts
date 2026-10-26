import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { mockStreamPlugin } from "./server/mockStream";

// 대시보드는 두 가지 원천을 볼 수 있다.
//
//   기본           목업 스트림 — 인프라 없이 돈다. 실 nuScenes 픽스처를 재생하므로
//                  난수가 아니고 화면·성능은 실제와 같다
//   VITE_API=...   실 API — Spring Boot(ClickHouse 질의 + SSE). 파이프라인이
//                  실행 중이어야 한다.
//
// **프론트 코드는 어느 쪽인지 모른다.** 경로(/api/...)와 응답 형태가 같아서
// 여기 프록시만 바꾸면 된다 — 그게 API 를 목업 계약에 맞춘 이유다.
const apiTarget = process.env.VITE_API;

export default defineConfig({
  // Keep the viewer's import.meta.url relative to its WASM asset in development.
  optimizeDeps: { exclude: ["@rerun-io/web-viewer"] },
  plugins: [react(), ...(apiTarget ? [] : [mockStreamPlugin()])],
  server: {
    port: 5173,
    proxy: apiTarget
      ? {
          "/api": {
            target: apiTarget,
            changeOrigin: true,
            // SSE 는 응답을 버퍼링하면 안 된다 — 프록시가 모아뒀다 보내면
            // 실시간이 아니게 된다.
            configure: (proxy) => {
              proxy.on("proxyRes", (proxyRes) => {
                proxyRes.headers["cache-control"] = "no-cache";
              });
            },
          },
        }
      : undefined,
  },
  build: {
    target: "es2022",
    rollupOptions: {
      input: { main: "index.html", replay: "replay.html" },
      output: {
        // 벤더를 분리한다. 앱 코드는 자주 바뀌지만 지도·차트 라이브러리는 거의 안 바뀌므로,
        // 한 청크에 묶어두면 앱을 배포할 때마다 1.3MB를 다시 받게 된다.
        // Rerun 뷰어(wasm 29.8MB)는 동적 import라 자동으로 별도 청크가 된다 — 여기 넣지 않는다.
        manualChunks: {
          maplibre: ["maplibre-gl"],
          charts: ["uplot"],
          react: ["react", "react-dom", "react-dom/client"],
        },
      },
    },
  },
});
