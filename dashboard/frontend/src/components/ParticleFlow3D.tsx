import { useEffect, useRef } from "react";
import * as THREE from "three";
import { MetricsSnapshot } from "../types";

const NUM_PARTITIONS = 4;
const MAX_PARTICLES = 1800;
const MIN_ACTIVE = 250;

const COLORS = {
  PASS: new THREE.Color("#22c55e"),
  ESCALATE: new THREE.Color("#eab308"),
  BLOCK: new THREE.Color("#ef4444"),
  pipe: new THREE.Color("#3b4252"),
};

type Bucket = "PASS" | "ESCALATE" | "BLOCK";

const BUCKETS: Bucket[] = ["PASS", "ESCALATE", "BLOCK"];
const BUCKET_Y: Record<Bucket, number> = { PASS: 3.2, ESCALATE: 0, BLOCK: -3.2 };

interface Particle {
  partition: number;
  bucket: Bucket;
  t: number;
  speed: number;
}

function buildSourceCurve(partitionIdx: number): THREE.CatmullRomCurve3 {
  const y = -3 + (partitionIdx * 2);
  return new THREE.CatmullRomCurve3([
    new THREE.Vector3(-10, y, (partitionIdx - 1.5) * 0.6),
    new THREE.Vector3(-6, y * 0.6, (partitionIdx - 1.5) * 0.4),
    new THREE.Vector3(-2, y * 0.15, 0),
    new THREE.Vector3(0, 0, 0),
  ]);
}

function buildDestCurve(bucket: Bucket): THREE.CatmullRomCurve3 {
  const targetY = BUCKET_Y[bucket];
  return new THREE.CatmullRomCurve3([
    new THREE.Vector3(0, 0, 0),
    new THREE.Vector3(3, targetY * 0.4, 0),
    new THREE.Vector3(6, targetY * 0.85, 0),
    new THREE.Vector3(8.5, targetY, 0),
  ]);
}

export function ParticleFlow3D({ snapshot }: { snapshot: MetricsSnapshot }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const snapshotRef = useRef(snapshot);
  snapshotRef.current = snapshot;

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    const scene = new THREE.Scene();
    scene.background = new THREE.Color("#0b0f14");

    const camera = new THREE.PerspectiveCamera(
      50,
      container.clientWidth / container.clientHeight,
      0.1,
      100
    );

    const renderer = new THREE.WebGLRenderer({ antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setSize(container.clientWidth, container.clientHeight);
    container.appendChild(renderer.domElement);

    scene.add(new THREE.AmbientLight(0xffffff, 0.6));
    const dirLight = new THREE.DirectionalLight(0xffffff, 0.8);
    dirLight.position.set(5, 10, 8);
    scene.add(dirLight);

    const sourceCurves = Array.from({ length: NUM_PARTITIONS }, (_, i) => buildSourceCurve(i));
    const destCurves = Object.fromEntries(
      BUCKETS.map((b) => [b, buildDestCurve(b)])
    ) as Record<Bucket, THREE.CatmullRomCurve3>;

    // Putket (partitiot + reitit altaisiin), puoliläpinäkyvät
    for (const curve of sourceCurves) {
      const geo = new THREE.TubeGeometry(curve, 40, 0.08, 8, false);
      const mat = new THREE.MeshBasicMaterial({
        color: COLORS.pipe,
        transparent: true,
        opacity: 0.25,
      });
      scene.add(new THREE.Mesh(geo, mat));
    }
    for (const bucket of BUCKETS) {
      const geo = new THREE.TubeGeometry(destCurves[bucket], 40, 0.06, 8, false);
      const mat = new THREE.MeshBasicMaterial({
        color: COLORS[bucket],
        transparent: true,
        opacity: 0.15,
      });
      scene.add(new THREE.Mesh(geo, mat));
    }

    // Altaat (kolme väri-koodattua kohdetta)
    const pools: Record<string, THREE.Mesh> = {};
    for (const bucket of BUCKETS) {
      const geo = new THREE.SphereGeometry(0.6, 24, 24);
      const mat = new THREE.MeshStandardMaterial({
        color: COLORS[bucket],
        emissive: COLORS[bucket],
        emissiveIntensity: 0.5,
      });
      const mesh = new THREE.Mesh(geo, mat);
      mesh.position.set(8.5, BUCKET_Y[bucket], 0);
      scene.add(mesh);
      pools[bucket] = mesh;
    }

    // Partikkelit: yksi InstancedMesh, aktiivinen osajoukko piilotetaan skaalaamalla nollaan
    const particleGeo = new THREE.SphereGeometry(0.07, 8, 8);
    const particleMat = new THREE.MeshBasicMaterial({ vertexColors: false });
    const instanced = new THREE.InstancedMesh(particleGeo, particleMat, MAX_PARTICLES);
    instanced.instanceColor = new THREE.InstancedBufferAttribute(
      new Float32Array(MAX_PARTICLES * 3),
      3
    );
    scene.add(instanced);

    function pickBucket(): Bucket {
      const d = snapshotRef.current.decisions;
      const total = d.PASS + d.ESCALATE + d.BLOCK;
      if (total === 0) return "PASS";
      const r = Math.random() * total;
      if (r < d.PASS) return "PASS";
      if (r < d.PASS + d.ESCALATE) return "ESCALATE";
      return "BLOCK";
    }

    function spawn(): Particle {
      return {
        partition: Math.floor(Math.random() * NUM_PARTITIONS),
        bucket: pickBucket(),
        t: Math.random(),
        speed: 0.18 + Math.random() * 0.14,
      };
    }

    const particles: Particle[] = Array.from({ length: MAX_PARTICLES }, spawn);

    const dummy = new THREE.Object3D();
    const tmpColor = new THREE.Color();
    const clock = new THREE.Clock();

    function activeCount(): number {
      const rate = snapshotRef.current.producer_rate;
      return Math.max(MIN_ACTIVE, Math.min(MAX_PARTICLES, Math.round(rate * 0.22)));
    }

    let animationId: number;
    function animate() {
      animationId = requestAnimationFrame(animate);
      const delta = Math.min(clock.getDelta(), 0.1);
      const active = activeCount();

      for (let i = 0; i < MAX_PARTICLES; i++) {
        const p = particles[i];
        if (i >= active) {
          dummy.position.set(0, 0, -1000);
          dummy.scale.setScalar(0);
          dummy.updateMatrix();
          instanced.setMatrixAt(i, dummy.matrix);
          continue;
        }

        p.t += p.speed * delta;
        if (p.t >= 1) {
          particles[i] = spawn();
          continue;
        }

        let pos: THREE.Vector3;
        let color: THREE.Color;
        if (p.t < 0.5) {
          pos = sourceCurves[p.partition].getPoint(p.t * 2);
          color = COLORS.pipe;
        } else {
          pos = destCurves[p.bucket].getPoint((p.t - 0.5) * 2);
          color = COLORS[p.bucket];
        }

        dummy.position.copy(pos);
        dummy.scale.setScalar(1);
        dummy.updateMatrix();
        instanced.setMatrixAt(i, dummy.matrix);
        tmpColor.copy(color);
        instanced.setColorAt(i, tmpColor);
      }
      instanced.instanceMatrix.needsUpdate = true;
      if (instanced.instanceColor) instanced.instanceColor.needsUpdate = true;

      // Altaiden pulssi piikin aikana
      const spike = snapshotRef.current.producer_mode === "spike";
      for (const bucket of BUCKETS) {
        const target = spike ? 1.15 : 1.0;
        pools[bucket].scale.lerp(new THREE.Vector3(target, target, target), 0.05);
      }

      const angle = clock.elapsedTime * 0.05;
      camera.position.set(Math.sin(angle) * 14, 2.5, Math.cos(angle) * 14);
      camera.lookAt(1, 0, 0);

      renderer.render(scene, camera);
    }
    animate();

    function handleResize() {
      if (!container) return;
      camera.aspect = container.clientWidth / container.clientHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(container.clientWidth, container.clientHeight);
    }
    window.addEventListener("resize", handleResize);

    return () => {
      cancelAnimationFrame(animationId);
      window.removeEventListener("resize", handleResize);
      renderer.dispose();
      container.removeChild(renderer.domElement);
    };
  }, []);

  // Kanvas on ruudunlukijalle läpinäkymätön pikselikartta — data on olemassa
  // myös LiveAnnouncerissa ja AccessibleDataTablessa, joten tämä on turvallista
  // piilottaa avustavalta teknologialta kahdesti kerrotun sisällön sijaan.
  return <div ref={containerRef} className="particle-stream" aria-hidden="true" role="presentation" />;
}
