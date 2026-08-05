package com.fanqie;

import com.github.unidbg.AndroidEmulator;
import com.github.unidbg.Emulator;
import com.github.unidbg.Module;
import com.github.unidbg.file.FileResult;
import com.github.unidbg.file.IOResolver;
import com.github.unidbg.file.linux.AndroidFileIO;
import com.github.unidbg.linux.android.AndroidEmulatorBuilder;
import com.github.unidbg.linux.android.AndroidResolver;
import com.github.unidbg.linux.android.dvm.*;
import com.github.unidbg.linux.file.ByteArrayFileIO;
import com.github.unidbg.memory.Memory;
import com.github.unidbg.arm.backend.Unicorn2Factory;
import java.io.File;
import java.security.MessageDigest;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.nio.file.Files;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

@SuppressWarnings({"rawtypes","unchecked"})
public class TomatoSigner extends AbstractJni {

    private final AndroidEmulator emulator;
    private final VM vm;
    private Module metasecModule;
    private DvmClass y2Class;
    private long fakeContextAddr = 0;
    private com.github.unidbg.Module ttCryptoModule;
    private long insnCount = 0;
    private boolean tracingType = false;
    private final long[] capturedFakeKeyAddr = {0L};
    private final boolean[] initWriteMonitor = {false};

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
    private long[] recentPC = new long[32];
    private int pcIdx = 0;

    private String dumpRecentPC() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < recentPC.length; i++) {
            int idx = (pcIdx + i) % recentPC.length;
            sb.append("0x").append(Long.toHexString(recentPC[idx])).append(" ");
        }
        return sb.toString();
    }

    private String hexDump(byte[] data, int offset, int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            if (i % 16 == 0) sb.append(String.format("\n  +%03x: ", offset + i));
            sb.append(String.format("%02x ", data[i] & 0xFF));
        }
        return sb.toString();
    }

    public TomatoSigner() {
        String baseDir = "/storage/emulated/0/Download/杂案/";
        String apkOverride = "/storage/emulated/0/Download/novelapp_49786187a_v7287_70932_70932_b302_1770344125.apk";
        String apkPath = apkOverride;
        String soDir = baseDir + "arm64-v8a/";

        emulator = AndroidEmulatorBuilder.for64Bit()
                .setProcessName("com.dragon.read")
                .addBackendFactory(new Unicorn2Factory(true))
                .build();
        emulator.getSyscallHandler().setEnableThreadDispatcher(true);

        Memory memory = emulator.getMemory();
        memory.setLibraryResolver(new AndroidResolver(23));

        vm = emulator.createDalvikVM(new File(apkPath));
        vm.setJni(this);
        vm.setVerbose(true);

        // ========== IOResolver ==========
        emulator.getSyscallHandler().addIOResolver(new IOResolver() {
            public FileResult resolve(Emulator emulator, String pathname, int oflags) {
                System.out.println("[IO open] " + pathname);
                if ("/proc/stat".equals(pathname)) {
                    byte[] d = "cpu  23032 0 45780 1203020 0 0 0 0 0 0\n".getBytes();
                    return FileResult.<AndroidFileIO>success(new ByteArrayFileIO(oflags, pathname, d));
                }
                if ("/proc/self/exe".equals(pathname) || pathname.contains("libc.so")) {
                    try {
                        File dummyElf = new File(soDir + "libmetasec_ml.so");
                        if (dummyElf.exists()) {
                            byte[] elfBytes = Files.readAllBytes(dummyElf.toPath());
                            if ("/proc/self/exe".equals(pathname) && elfBytes.length > 17) {
                                elfBytes[16] = 0x02;
                                elfBytes[17] = 0x00;
                                System.out.println("  -> 伪造 ELF (ET_EXEC): " + pathname + " (长度:" + elfBytes.length + ")");
                            } else {
                                System.out.println("  -> 返回 ELF (原样): " + pathname + " (长度:" + elfBytes.length + ")");
                            }
                            return FileResult.<AndroidFileIO>success(new ByteArrayFileIO(oflags, pathname, elfBytes));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return null;
            }
        });

        DvmClass objClass = vm.resolveClass("java/lang/Object");
        y2Class = vm.resolveClass("ms/bd/c/y2", objClass);
        DvmClass i2Class = vm.resolveClass("ms/bd/c/i2", y2Class);
        vm.resolveClass("com/bytedance/mobsec/metasec/ml/MS", i2Class);

        long stubAddr = memory.malloc(16, true).getPointer().peer;
        long vtableAddr = memory.malloc(4096, true).getPointer().peer;
        long ctxAddr = memory.malloc(4096, true).getPointer().peer;
        emulator.getBackend().mem_write(stubAddr, new byte[]{(byte)0xC0, 0x03, 0x5F, (byte)0xD6});

        long slot6Stub = memory.malloc(16, true).getPointer().peer;
        emulator.getBackend().mem_write(slot6Stub, new byte[]{
            (byte)0x20, 0x00, 0x00, (byte)0xF9,
            0x00, 0x00, (byte)0x80, (byte)0xD2,
            (byte)0xC0, 0x03, 0x5F, (byte)0xD6
        });

        byte[] vtable = new byte[4096];
        for (int i = 0; i < 512; i++) {
            long ptr = (i == 6) ? slot6Stub : stubAddr;
            for (int b = 0; b < 8; b++)
                vtable[i*8+b] = (byte)(ptr >> (b*8));
        }
        emulator.getBackend().mem_write(vtableAddr, vtable);

        long innerObj = memory.malloc(4096, true).getPointer().peer;
        byte[] ctx = new byte[4096];
        for (int b = 0; b < 8; b++) ctx[b] = (byte)(vtableAddr >> (b*8));
        for (int b = 0; b < 8; b++) ctx[72+b] = (byte)(innerObj >> (b*8));
        emulator.getBackend().mem_write(ctxAddr, ctx);
        fakeContextAddr = ctxAddr;

        DalvikModule ttCryptoDm = vm.loadLibrary(new File(soDir + "libttcrypto.so"), true);
        ttCryptoModule = ttCryptoDm.getModule();
        System.out.println("libttcrypto.so 加载基址 = 0x" + Long.toHexString(ttCryptoModule.base));

        DalvikModule dm = vm.loadLibrary(new File(soDir + "libmetasec_ml.so"), true);
        metasecModule = dm.getModule();
        long base = metasecModule.base;

        // SM4 S-box hook
        long sm4SboxRuntimeAddr = ttCryptoModule.base + 0x5b678L;
        System.out.println("SM4 S-box 运行时地址 = 0x" + Long.toHexString(sm4SboxRuntimeAddr));
        final boolean[] sboxHookFired = {false};
        emulator.getBackend().hook_add_new(
            new com.github.unidbg.arm.backend.ReadHook() {
                @Override
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    try {
                        long lr = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                        long lrOffsetInTtCrypto = lr - ttCryptoModule.base;
                        long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                        long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                        long x2 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2)).longValue();
                        long sp = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_SP)).longValue();
                        System.out.println("[S-box命中] LR=0x" + Long.toHexString(lrOffsetInTtCrypto)
                            + "(libttcrypto内偏移) x0=0x" + Long.toHexString(x0)
                            + " x1=0x" + Long.toHexString(x1) + " x2=0x" + Long.toHexString(x2)
                            + " sp=0x" + Long.toHexString(sp));
                        if (!sboxHookFired[0]) {
                            sboxHookFired[0] = true;
                            byte[] stackDump = emulator.getBackend().mem_read(sp, 64);
                            System.out.println("  [栈dump sp+0~64] " + bytesToHex(stackDump));
                        }
                    } catch(Exception e) {
                        System.out.println("[S-box命中] hook异常: " + e);
                    }
                }
                @Override public void detach(){}
                @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, sm4SboxRuntimeAddr, sm4SboxRuntimeAddr + 256, null);

        // ===== ttcrypto 签名函数入口追踪 (0x231c80) =====
        final long ttSignFnAddr = ttCryptoModule.base + 0x231c80L;
        System.out.println("ttcrypto签名函数地址 = 0x" + Long.toHexString(ttSignFnAddr));
        final boolean[] ttSignFired = {false};
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address != ttSignFnAddr) return;
                try {
                    long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                    long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                    long x2 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2)).longValue();
                    long x3 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X3)).longValue();
                    long x4 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X4)).longValue();
                    long x5 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X5)).longValue();
                    long lr = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                    System.out.println("\n========== [ttcrypto+0x231c80] 签名函数被调用 ==========");
                    System.out.printf("  x0=0x%x x1=0x%x x2=0x%x x3=0x%x x4=0x%x x5=0x%x%n", x0, x1, x2, x3, x4, x5);
                    System.out.printf("  LR=0x%x (metasec偏移=0x%x)%n", lr, lr - metasecModule.base);
                    long[] regs = {x0, x1, x2, x3, x4, x5};
                    String[] names = {"x0", "x1", "x2", "x3", "x4", "x5"};
                    for (int i = 0; i < regs.length; i++) {
                        if (regs[i] > 0x10000000L && regs[i] < 0x40000000L) {
                            try {
                                byte[] d = backend.mem_read(regs[i], 64);
                                System.out.println("  [" + names[i] + " ptr dump 64B] " + bytesToHex(d));
                            } catch(Exception ex) {
                                System.out.println("  [" + names[i] + " ptr dump 失败] " + ex.getMessage());
                            }
                        }
                    }
                    try {
                        byte[] ctxNow = backend.mem_read(fakeContextAddr, 128);
                        System.out.println("  [fakeContextAddr当前状态 128B]" + hexDump(ctxNow, 0, 128));
                    } catch(Exception ex) {
                        System.out.println("  [fakeContextAddr dump失败] " + ex.getMessage());
                    }
                    if (!ttSignFired[0]) {
                        ttSignFired[0] = true;
                        System.out.println("  [反汇编 ttcrypto+0x231c80 前64B]");
                        emulator.printAssemble(System.out, ttSignFnAddr, 0x40, 0, null);
                    }
                    System.out.println("====================================================\n");
                } catch(Exception e) {
                    System.out.println("[ttcrypto签名函数hook异常] " + e);
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, ttSignFnAddr, ttSignFnAddr + 4, null);

        // ========== 常规补丁 & 核心修复 ==========
        try {
            emulator.getBackend().mem_protect(base, metasecModule.size, 7);
            emulator.getBackend().mem_write(base + 0x26e428, new byte[]{0x1f, 0x20, 0x03, (byte)0xd5});
            emulator.getBackend().mem_write(base + 0x26e7c8, new byte[]{0x1f, 0x20, 0x03, (byte)0xd5});
            emulator.getBackend().mem_write(base + 0x26e7e8, new byte[]{0x1f, 0x20, 0x03, (byte)0xd5});
            emulator.getBackend().mem_write(base + 0x27184c, new byte[]{0x01, 0x00, (byte)0x80, 0x52, (byte)0xC0, 0x03, 0x5f, (byte)0xD6});
            
            System.out.println("✅ 已注释 26ffb8/26ffcc 的 NOP，JNI Release 将正常执行");
            emulator.getBackend().mem_write(base + 0x173788L, new byte[]{(byte)0xe0, (byte)0xa7, 0x02, (byte)0x54});
            
            // ---- 运行时覆盖 0x26c9d0-0x26ca10，绕过 JIT 缓存 ----
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    try {
                        if (off == 0x26c9fcL) {
                            long x8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue();
                            long x9 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X9).longValue();
                            System.out.printf("[0x26c9fc] ✅ REACHED! x8(handler)=0x%x x9(table)=0x%x%n", x8, x9);
                            if (x8 == 0L) {
                                long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                                long typeId = java.nio.ByteBuffer.wrap(backend.mem_read(x0 + 0x10, 8))
                                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                                int idx = (int)((typeId >> 4) & 0xFF);
                                if (x9 != 0) {
                                    long h = java.nio.ByteBuffer.wrap(backend.mem_read(x9 + idx * 8L, 8))
                                        .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                                    System.out.printf("  -> table[%d]=0x%x (%s)%n", idx, h, h==0?"NULL!":"写入寄存器");
                                    if (h != 0) backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X8, h);
                                }
                            }
                        } else if (off == 0x26ca04L) {
                            long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                            System.out.printf("[0x26ca04] ✅ b #0x126c7f10 将执行! x0=0x%x%n", x0);
                        }
                    } catch(Exception e) {
                        System.out.println("[0x26c9d0 hook异常] " + e);
                    }
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x26c9d0L, metasecModule.base + 0x26ca10L, null);
            System.out.println("✅ 添加 runtime hook 覆盖 0x26c9d0-0x26ca10，绕过 JIT 缓存");            
            
            System.out.println("✅ patch 173788: 还原 B.EQ (保留失败出口)");

            System.out.println("ℹ️ 已撤销 17bcc0 的 NOP 补丁，保留原指令用于追踪");
            System.out.println("ℹ️ 已撤销 0x17bcdc 的 NOP（根据最终分析，真正的问题不在此处）");

            emulator.getBackend().mem_write(base + 0x26e8f4L, new byte[]{0x1f, 0x20, 0x03, (byte)0xd5});
            System.out.println("✅ patch 26e8f4: bl -> nop (跳过真正的环境检查)");

            // ========== 撤销 0x270164 补丁，恢复原指令 b 0x1625a4 ==========
            emulator.getBackend().mem_write(base + 0x270164L, new byte[]{0x10, (byte)0xc9, (byte)0xfb, 0x17});
            System.out.println("✅ 还原 270164 原指令 b 0x1625a4");

            // ========== 拦截 BRK 反调试中断 ==========
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.InterruptHook() {
                @Override
                public void hook(com.github.unidbg.arm.backend.Backend backend, int intno, int swi, Object user) {
                    try {
                        long pc = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_PC).longValue();
                        byte[] bytes = backend.mem_read(pc, 4);
                        int insn = (bytes[0] & 0xFF) | ((bytes[1] & 0xFF) << 8) | ((bytes[2] & 0xFF) << 16) | ((bytes[3] & 0xFF) << 24);
                        System.out.println("[InterruptHook] intno=" + intno + " swi=" + swi + " PC=0x" + Long.toHexString(pc) + " insn=0x" + Integer.toHexString(insn));
                        if ((insn & 0xFFE0001F) == 0xD4200000) {
                            System.out.println("  -> 检测到 BRK 反调试指令，自动跳过!");
                            backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_PC, pc + 4);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                @Override public void detach(){}
                @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, null);
            System.out.println("✅ 已挂载 InterruptHook 拦截 BRK 反调试");

            // 0x26e684 入口 hook：重置计数器，启用追踪
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    if (off == 0x26e684L) {
                        insnCount = 0;
                        tracingType = true;
                    }
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, base + 0x26e684L, base + 0x26e684L + 4, null);

            // 指令计数器 + 滚动记录最近32条指令地址
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    if (!tracingType) return;
                    insnCount++;
                    recentPC[pcIdx] = address - metasecModule.base;
                    pcIdx = (pcIdx + 1) % recentPC.length;
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, base, base + metasecModule.size, null);

            // 0x26e920: mov x0, x20 前打印最终返回值
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    if (!tracingType) return;
                    long x20 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20)).longValue();
                    System.out.println("  [26e920] 函数即将返回, x20(最终x0)=0x" + Long.toHexString(x20));
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, base + 0x26e920L, base + 0x26e920L + 4, null);

        } catch (Exception e) { e.printStackTrace(); }

        // ========== Fix 2 & 3：监控初始化写入 & Hook 真正的初始化函数 ==========
        initWriteMonitor[0] = true;
        System.out.println("  [init期间写入监控已开启(JNI_OnLoad)]");

        final long initFn1 = metasecModule.base + 0x17bc34L;
        final long initFn2 = metasecModule.base + 0x2a8d60L;
        final long afterInit2 = metasecModule.base + 0x17bce0L;
        System.out.println("初始化函数1地址(0x17bc34) = 0x" + Long.toHexString(initFn1));
        System.out.println("初始化函数2地址(0x2a8d60) = 0x" + Long.toHexString(initFn2));

        // Hook 0x17bc34 和 0x17bce0
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            boolean fired1 = false;
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                try {
                    if (address == initFn1) {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                        long x2 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2).longValue();
                        System.out.printf("%n===== [0x17bc34] 初始化函数1被调用 =====%n");
                        System.out.printf("  x0=0x%x x1=0x%x x2=0x%x%n", x0, x1, x2);
                        if (x0 > 0x10000000L && x0 < 0x40000000L) {
                            byte[] d = backend.mem_read(x0, 128);
                            System.out.println("  [x0 dump 128B] " + bytesToHex(d));
                        }
                        if (!fired1) {
                            fired1 = true;
                            System.out.println("  [反汇编 0x17bc34 前64B]");
                            emulator.printAssemble(System.out, initFn1, 0x40, 0, null);
                        }
                    } else if (address == afterInit2) {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        long x19 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19).longValue();
                        System.out.printf("[0x17bce0] 0x2a8d60返回后, x0=0x%x, x19=0x%x%n", x0, x19);
                        if (x0 > 0x10000000L && x0 < 0x40000000L) {
                            byte[] d = backend.mem_read(x0, 128);
                            System.out.println("  [x0 dump 128B] " + bytesToHex(d));
                        }
                    }
                } catch(Exception e) { System.out.println("[init hook异常] " + e); }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, initFn1, afterInit2 + 4, null);

        // ========== 单独 Hook 0x2a8d60：强制修正 x2 = fakeContextAddr ==========
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            boolean fired2 = false;
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address != initFn2) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long x2 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2).longValue();
                    System.out.printf("%n===== [0x2a8d60] 初始化函数2被调用 =====%n");
                    System.out.printf("  原 x0=0x%x x1=0x%x x2=0x%x%n", x0, x1, x2);

                    if (x2 == 0) {
                        backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X2, fakeContextAddr);
                        System.out.println("  -> 检测到 x2=0, 强制修正为 fakeContextAddr=0x" + Long.toHexString(fakeContextAddr));
                    }

                    if (x0 > 0x10000000L && x0 < 0x40000000L) {
                        byte[] d = backend.mem_read(x0, 128);
                        System.out.println("  [x0 dump 128B] " + bytesToHex(d));
                    }
                    if (!fired2) {
                        fired2 = true;
                        System.out.println("  [反汇编 0x2a8d60 前64B]");
                        emulator.printAssemble(System.out, initFn2, 0x40, 0, null);
                    }
                } catch(Exception e) { System.out.println("[0x2a8d60 hook异常] " + e); }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, initFn2, initFn2 + 4, null);

        // ========== 新增：绕过 0x2a8d60 内部的 CallBooleanMethodV 校验 ==========
        final long afterBoolCall = metasecModule.base + 0x2a8da4L;
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address == afterBoolCall) {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    if (x0 == 0) {
                        System.out.println("[0x2a8da4] 检测到 CallBooleanMethodV 返回 0, 强制改为 1");
                        backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X0, 1L);
                    }
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, afterBoolCall, afterBoolCall + 4, null);

        // ========== 新增：绕过 0x2a8d60 内部的 JNIEnv 混淆调用 ==========
        final long stubReturn0 = memory.malloc(16, true).getPointer().peer;
        emulator.getBackend().mem_write(stubReturn0, new byte[]{0x00, 0x00, (byte)0x80, (byte)0xD2, (byte)0xC0, 0x03, 0x5F, (byte)0xD6});
        
        final long confuseCallAddr = metasecModule.base + 0x2a8da0L;
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address == confuseCallAddr) {
                    try {
                        long x8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue();
                        if (x8 > 0xfffe0000L && x8 < 0xffff0000L) {
                            System.out.println("[0x2a8da0] 检测到 JNIEnv 混淆调用, x8=0x" + Long.toHexString(x8) + " -> 重定向到 stubReturn0");
                            backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X8, stubReturn0);
                        }
                    } catch(Exception e) {}
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, confuseCallAddr, confuseCallAddr + 4, null);

        // 写入监控 (WriteHook)
        emulator.getBackend().hook_add_new(
            new com.github.unidbg.arm.backend.WriteHook() {
                @Override
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, long value, Object user) {
                    if (!initWriteMonitor[0]) return;
                    boolean inMetasec = address >= metasecModule.base && address < metasecModule.base + metasecModule.size;
                    boolean inTtcrypto = address >= ttCryptoModule.base && address < ttCryptoModule.base + ttCryptoModule.size;
                    boolean isStack = address >= 0xe0000000L;
                    if ((inMetasec || inTtcrypto) && !isStack) {
                        long pcOff = 0;
                        try {
                            long pc = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_PC)).longValue();
                            pcOff = inMetasec ? pc - metasecModule.base : pc - ttCryptoModule.base;
                        } catch(Exception ex) {}
                        String lib = inMetasec ? "metasec" : "ttcrypto";
                        long addrOff = inMetasec ? address - metasecModule.base : address - ttCryptoModule.base;
                        System.out.printf("  [init写入/%s] +0x%x = 0x%x (size=%d, PC_off=0x%x)%n",
                            lib, addrOff, value, size, pcOff);
                    }
                }
                @Override public void detach(){}
                @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, 0x12000000L, 0x13000000L, null);

        // ========== 调用 JNI_OnLoad ==========
        try {
            dm.callJNI_OnLoad(emulator);
        } catch (Exception e) {
            System.out.println("JNI_OnLoad 警告: " + e.getMessage());
        }
    }

    // ---------- XGorgon 算法实现 ----------
    private static class XGorgon {
        private final int length = 20;
        private final int[] hex_digits = {30, 64, 224, 217, 147, 69, 0, 180};

        private int[] encryption() {
            int[] hex_array = new int[256];
            for (int i = 0; i < 256; i++) hex_array[i] = i;
            int temp = -1;
            for (int i = 0; i < 256; i++) {
                int A = (i == 0) ? 0 : (temp != -1 ? temp : hex_array[i - 1]);
                int B = hex_digits[i % 8];
                if (A == 85 && i != 1 && temp != 85) A = 0;
                int C = (A + i + B) % 256;
                temp = (C < i) ? C : -1;
                int D = hex_array[C];
                hex_array[i] = D;
            }
            return hex_array;
        }

        private int[] initialize(int[] input_list, int[] hex_array) {
            List<Integer> temp_add = new ArrayList<>();
            int[] temp_hex = hex_array.clone();
            for (int i = 0; i < length; i++) {
                int A = input_list[i];
                int B = temp_add.isEmpty() ? 0 : temp_add.get(temp_add.size() - 1);
                int C = (hex_array[i + 1] + B) % 256;
                temp_add.add(C);
                int D = temp_hex[C];
                temp_hex[i + 1] = D;
                int E = (D + D) % 256;
                int F = temp_hex[E];
                input_list[i] = A ^ F;
            }
            return input_list;
        }

        private int[] handle(int[] input_list) {
            for (int i = 0; i < length; i++) {
                int A = input_list[i];
                int B = reverse(A);
                int C = input_list[(i + 1) % length];
                int D = B ^ C;
                int E = RBIT(D);
                int F = E ^ length;
                int G = ~F;
                String hexG = Integer.toHexString(G);
                if (hexG.length() > 2) hexG = hexG.substring(hexG.length() - 2);
                input_list[i] = Integer.parseInt(hexG, 16);
            }
            return input_list;
        }

        private int reverse(int num) {
            String tmp = Integer.toHexString(num);
            if (tmp.length() < 2) tmp = "0" + tmp;
            return Integer.parseInt(tmp.substring(1) + tmp.substring(0, 1), 16);
        }

        private int RBIT(int num) {
            String tmp = Integer.toBinaryString(num);
            while (tmp.length() < 8) tmp = "0" + tmp;
            StringBuilder res = new StringBuilder();
            for (int i = 0; i < 8; i++) res.append(tmp.charAt(7 - i));
            return Integer.parseInt(res.toString(), 2);
        }

        private String hex2string(int num) {
            String tmp = Integer.toHexString(num);
            return (tmp.length() < 2) ? "0" + tmp : tmp;
        }

        public Map<String, String> calculate(String url, Map<String, String> headers) {
            int[] gorgon_list = new int[20];
            long ts = System.currentTimeMillis() / 1000;
            String timestamp_hex = Long.toHexString(ts);
            while (timestamp_hex.length() < 8) timestamp_hex = "0" + timestamp_hex;

            try {
                byte[] url_hash = MessageDigest.getInstance("MD5").digest(url.getBytes("UTF-8"));
                for (int i = 0; i < 4; i++) gorgon_list[i] = url_hash[i] & 0xFF;

                String stub = headers.get("x-ss-stub");
                if (stub != null) {
                    for (int i = 0; i < 4; i++) gorgon_list[4 + i] = Integer.parseInt(stub.substring(2 * i, 2 * i + 2), 16);
                }
                String cookie = headers.get("cookie");
                if (cookie != null) {
                    byte[] cookie_hash = MessageDigest.getInstance("MD5").digest(cookie.getBytes("UTF-8"));
                    for (int i = 0; i < 4; i++) gorgon_list[8 + i] = cookie_hash[i] & 0xFF;
                }
                for (int i = 0; i < 4; i++) gorgon_list[16 + i] = (int) Long.parseLong(timestamp_hex.substring(2 * i, 2 * i + 2), 16);

                int[] resList = handle(initialize(gorgon_list, encryption()));
                StringBuilder sb = new StringBuilder("0404");
                sb.append(hex2string(hex_digits[7])).append(hex2string(hex_digits[3]))
                  .append(hex2string(hex_digits[1])).append(hex2string(hex_digits[6]));
                for (int val : resList) sb.append(hex2string(val));

                Map<String, String> result = new HashMap<>();
                result.put("X-Gorgon", sb.toString());
                result.put("X-Khronos", String.valueOf(ts));
                return result;
            } catch (Exception e) { e.printStackTrace(); return null; }
        }
    }

    public void testSign() {
        System.out.println("\n--- 开始测试四神生成 ---");

        // ==================== 反汇编关键区域 ====================
        System.out.println("\n--- 反汇编 0x1a58dc (真实环境校验) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x1a58dcL, 0x80, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x17bce4 (分支目标) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x17bce4L, 0x40, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x17bca0 (成功路径) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x17bca0L, 0x44, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x17bd30 (最终汇总点) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x17bd30L, 0x40, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x17bc78 (真实环境检查) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x17bc78L, 0x60, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x17d578 (第二道环境检查) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x17d578L, 0x80, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x26e940 (主入口中产生 -1 的地方) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x26e940L, 0x50, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x26e8e0 (设置 q6 的地方) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x26e8e0L, 0x60, 0, null);
        System.out.println("====================================\n");

        System.out.println("\n--- 反汇编 0x173780 (spin wait) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x173780L, 0x10, 0, null);

        System.out.println("\n--- 反汇编 0x177c80 (初始化分支) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x177c80L, 0x20, 0, null);

        System.out.println("\n--- 反汇编 0x26e980 (valueOf 调用点) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x26e980L, 0x40, 0, null);
        System.out.println("====================================\n");

        byte[] inst26e9bc = emulator.getBackend().mem_read(metasecModule.base + 0x26e9bc, 4);
        System.out.println("[0x26e9bc] 指令字节: " + bytesToHex(inst26e9bc));
        byte[] inst177c8c = emulator.getBackend().mem_read(metasecModule.base + 0x177c8c, 4);
        System.out.println("[0x177c8c] 指令字节: " + bytesToHex(inst177c8c));

        // ===== 反汇编真实签名函数 (ttcrypto+0x1f3d5c) =====
        System.out.println("\n--- ttcrypto+0x1f3d5c (真实签名函数) ---");
        emulator.printAssemble(System.out, ttCryptoModule.base + 0x1f3d5cL, 0x80, 0, null);
        System.out.println("---");

        // ==================== BL/BLR 追踪（主入口 0x26e684） ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off >= 0x26e684L && off < 0x26ea00L) {
                    byte[] inst = backend.mem_read(address, 4);
                    if ((inst[3] & 0xFC) == 0x94) {
                        int imm = ((inst[3] & 0x03) << 24) | ((inst[2] & 0xFF) << 16) | ((inst[1] & 0xFF) << 8) | (inst[0] & 0xFF);
                        if ((imm & 0x02000000) != 0) imm |= 0xFC000000;
                        long target = address + (imm * 4);
                        System.out.println("[0x" + Long.toHexString(off) + "] BL 0x" + Long.toHexString(target - metasecModule.base));
                    } else if ((inst[3] & 0xFF) == 0xD6 && (inst[2] & 0xFF) == 0x3F) {
                        long reg = ((inst[1] & 0x03) << 3) | ((inst[0] >> 5) & 0x07);
                        long val = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0 + (int)reg)).longValue();
                        System.out.println("[0x" + Long.toHexString(off) + "] BLR X" + reg + "=0x" + Long.toHexString(val));
                    }
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26e684L, metasecModule.base + 0x26ea00L, null);

        // ==================== BL/BLR 追踪（初始化逻辑 0x177c8c） ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off >= 0x177c8cL && off < 0x178c84L) {
                    byte[] inst = backend.mem_read(address, 4);
                    if ((inst[3] & 0xFC) == 0x94) {
                        int imm = ((inst[3] & 0x03) << 24) | ((inst[2] & 0xFF) << 16) | ((inst[1] & 0xFF) << 8) | (inst[0] & 0xFF);
                        if ((imm & 0x02000000) != 0) imm |= 0xFC000000;
                        long target = address + (imm * 4);
                        System.out.println("[0x" + Long.toHexString(off) + "] BL 0x" + Long.toHexString(target - metasecModule.base));
                    } else if ((inst[3] & 0xFF) == 0xD6 && (inst[2] & 0xFF) == 0x3F) {
                        long reg = ((inst[1] & 0x03) << 3) | ((inst[0] >> 5) & 0x07);
                        long val = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0 + (int)reg)).longValue();
                        System.out.println("[0x" + Long.toHexString(off) + "] BLR X" + reg + "=0x" + Long.toHexString(val));
                    }
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x177c8cL, metasecModule.base + 0x178c84L, null);

        // ==================== 0x177c8c BLR 目标反汇编 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x177c8cL) {
                    long x8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue();
                    long targetOff = x8 - metasecModule.base;
                    System.out.println("\n>>> [0x177c8c] BLR 目标偏移 = 0x" + Long.toHexString(targetOff) + " <<<");
                    System.out.println(">>> 目标函数反汇编：");
                    emulator.printAssemble(System.out, x8, 0x40, 0, null);
                    System.out.println("====================================\n");
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x177c8cL, metasecModule.base + 0x177c90L, null);

        // ==================== 追踪 RET 返回 -1 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off >= 0x26e684L && off < 0x26e9bcL) {
                    byte[] inst = backend.mem_read(address, 4);
                    if (inst[0] == (byte)0xC0 && inst[1] == 0x03 && inst[2] == 0x5F && inst[3] == (byte)0xD6) {
                        long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                        if (x0 == 0xffffffffL || x0 == -1L) {
                            System.out.println("[0x" + Long.toHexString(off) + "] RET 返回了 -1 !!!");
                        }
                    }
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26e684L, metasecModule.base + 0x26e9bcL, null);

        // ==================== 强制通过环境校验 0x17bc84 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x17bc84L) {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    System.out.println("[0x17bc84] 环境校验函数原返回 x0=0x" + Long.toHexString(x0) + " -> 强制改为 1");
                    backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X0, 1L);
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x17bc84L, metasecModule.base + 0x17bc88L, null);

        // ==================== 追踪成功路径 0x17bca0 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x17bca0L) System.out.println(">>> [0x17bca0] 成功路径被触发! <<<");
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x17bca0L, metasecModule.base + 0x17bca4L, null);

        // ==================== 追踪失败路径 0x17bce4 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x17bce4L) System.out.println(">>> [0x17bce4] 失败路径被触发! <<<");
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x17bce4L, metasecModule.base + 0x17bce8L, null);

        // ==================== 0x17d578 返回值追踪 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x17bcbcL) {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    System.out.println("[0x17d578返回] x0=0x" + Long.toHexString(x0));
                    if (x0 == 0xffffffffL || x0 == -1L) {
                        System.out.println("  -> 检测到 -1，强制改为 0");
                        backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X0, 0L);
                    }
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x17bcbcL, metasecModule.base + 0x17bcc0L, null);

        // ==================== 强制通过第二道环境校验 0x26e8f8 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x26e8f8L) {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    System.out.println("[0x26e8f8] 第二道校验原返回 x0=0x" + Long.toHexString(x0) + " -> 强制改为 0");
                    backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X0, 0L);
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26e8f8L, metasecModule.base + 0x26e8fcL, null);

        // ==================== 0x173788 spin-wait 状态追踪 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x173788L) {
                    byte[] inst = backend.mem_read(address, 4);
                    System.out.println("[0x173788] 指令字节: " + bytesToHex(inst)
                        + "  (LE)  解码提示: 14xxxxxx=B; 54xxxxxx=B.cond; 34/35=CBZ/CBNZ(32); b4/b5=CBZ/CBNZ(64)");
                    long x0  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                    long x1  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                    long x8  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                    long x9  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X9)).longValue();
                    long x19 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19)).longValue();
                    long x20 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20)).longValue();
                    long lr  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                    long nzcv = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_NZCV)).longValue();
                    System.out.println("  寄存器: x0=0x" + Long.toHexString(x0)
                        + " x1=0x" + Long.toHexString(x1)
                        + " x8=0x" + Long.toHexString(x8)
                        + " x9=0x" + Long.toHexString(x9)
                        + " x19=0x" + Long.toHexString(x19)
                        + " x20=0x" + Long.toHexString(x20)
                        + " LR=0x" + Long.toHexString(lr - metasecModule.base)
                        + " NZCV=0x" + Long.toHexString(nzcv));
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x173788L, metasecModule.base + 0x173790L, null);

        // ==================== 0x178c84 失败出口追踪 ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x178c84L) {
                    byte[] inst = backend.mem_read(address, 4);
                    System.out.println("[0x178c84] 指令字节: " + bytesToHex(inst));
                    long x0  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                    long x1  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                    long x8  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                    long x19 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19)).longValue();
                    long lr  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                    System.out.println("  寄存器: x0=0x" + Long.toHexString(x0)
                        + " x1=0x" + Long.toHexString(x1)
                        + " x8=0x" + Long.toHexString(x8)
                        + " x19=0x" + Long.toHexString(x19)
                        + " LR=0x" + Long.toHexString(lr - metasecModule.base));
                    byte[] next = backend.mem_read(address, 32);
                    System.out.println("  [0x178c84..+32] " + bytesToHex(next));
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x178c84L, metasecModule.base + 0x178c90L, null);

        // ==================== 追踪 0x15e1dc ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x15e1dcL) {
                    long x0  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                    long x1  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                    long x2  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2)).longValue();
                    long x8  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                    long x19 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19)).longValue();
                    long lr  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                    System.out.println("[0x15e1dc] x0=0x" + Long.toHexString(x0) + " x1=0x" + Long.toHexString(x1)
                        + " x2=0x" + Long.toHexString(x2) + " x8=0x" + Long.toHexString(x8)
                        + " x19=0x" + Long.toHexString(x19) + " LR=0x" + Long.toHexString(lr - metasecModule.base));
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x15e1dcL, metasecModule.base + 0x15e1e0L, null);

        // ==================== 追踪 0x176098 (Ladon 失败检查点) ====================
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off == 0x176098L) {
                    long x0  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                    long x1  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                    long x8  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                    long x19 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19)).longValue();
                    long lr  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                    System.out.println("[0x176098] x0=0x" + Long.toHexString(x0) + " x1=0x" + Long.toHexString(x1)
                        + " x8=0x" + Long.toHexString(x8) + " x19=0x" + Long.toHexString(x19)
                        + " LR=0x" + Long.toHexString(lr - metasecModule.base));
                    if (x1 > 0x10000000L && x1 < 0x40000000L) {
                        try {
                            byte[] d = backend.mem_read(x1, 64);
                            System.out.println("  [x1 ptr dump 64B] " + bytesToHex(d));
                        } catch(Exception ex) {}
                    }
                }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x176098L, metasecModule.base + 0x17609cL, null);

        String url = "https://novel.snssdk.com/api/novel/book/directory/list/v1/?device_id=1234567890";

        // 1. X-Gorgon & X-Khronos
        System.out.println("正在使用 api_fix 逻辑生成 X-Gorgon & X-Khronos...");
        XGorgon xg = new XGorgon();
        Map<String, String> xgResult = xg.calculate(url, new HashMap<>());
        if (xgResult != null) {
            System.out.println("  X-Gorgon: " + xgResult.get("X-Gorgon"));
            System.out.println("  X-Khronos: " + xgResult.get("X-Khronos"));
        }

        // 2. 初始化 SO 环境 (0x5000001)
        try {
            final int[] spinIterCount = {0};
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    try {
                        if (off == 0x173784L) {
                            spinIterCount[0]++;
                            long x8 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                            long x9 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X9)).longValue();
                            if (spinIterCount[0] <= 5 || spinIterCount[0] % 200 == 0) {
                                System.out.println("[spin#" + spinIterCount[0] + "] 0x173784 subs前 w8=0x"
                                    + Long.toHexString(x8 & 0xFFFFFFFFL) + " w9=0x" + Long.toHexString(x9 & 0xFFFFFFFFL));
                            }
                        } else if (off == 0x173770L || off == 0x173774L || off == 0x173778L || off == 0x17377cL) {
                            long x8  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                            long x9  = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X9)).longValue();
                            long x19 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19)).longValue();
                            long x20 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20)).longValue();
                            if (spinIterCount[0] <= 2) {
                                System.out.println("  [近似0x" + Long.toHexString(off) + "] x8=0x" + Long.toHexString(x8)
                                    + " x9=0x" + Long.toHexString(x9) + " x19=0x" + Long.toHexString(x19) + " x20=0x" + Long.toHexString(x20));
                            }
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x173760L, metasecModule.base + 0x173790L, null);

            y2Class.callStaticJniMethodObject(emulator, "a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;", 0x5000001, 0, 0L, null, null);
            initWriteMonitor[0] = false;
            System.out.println("  [init期间写入监控已关闭]");
            System.out.println("  [spin总迭代次数] " + spinIterCount[0]);
            System.out.println("  [指令计数] 初始化(0x5000001) 执行了 " + insnCount + " 条指令");

            System.out.println("\n========== [fakeContextAddr dump - init完成后] ==========");
            System.out.println("  fakeContextAddr = 0x" + Long.toHexString(fakeContextAddr));
            byte[] ctxFull = emulator.getBackend().mem_read(fakeContextAddr, 256);
            System.out.println("  [前256字节]" + hexDump(ctxFull, 0, 256));
            System.out.print("  [非零字节偏移] ");
            for (int i = 0; i < 256; i++) {
                if (ctxFull[i] != 0) System.out.printf("+%02x=%02x ", i, ctxFull[i] & 0xFF);
            }
            System.out.println();
            System.out.println("==========================================================\n");

            // ===== 新增：dispatch table 状态 dump =====
            try {
                System.out.println("\n===== dispatch table 状态 =====");
                long dispPtrAddr = metasecModule.base + 0x3deda8L;
                byte[] dpRaw = emulator.getBackend().mem_read(dispPtrAddr, 8);
                long dispPtr = ByteBuffer.wrap(dpRaw).order(ByteOrder.LITTLE_ENDIAN).getLong();
                System.out.printf("  [0x3deda8] dispatch_table ptr = 0x%x%n", dispPtr);
                if (dispPtr != 0 && dispPtr > 0x10000000L && dispPtr < 0x20000000L) {
                    byte[] table = emulator.getBackend().mem_read(dispPtr, 128);
                    System.out.println("  [dispatch table 前128B] " + bytesToHex(table));
                } else {
                    System.out.println("  !! dispatch table ptr 为空或无效，签名 handler 从未注册 !!");
                }
                long dtPtr2 = java.nio.ByteBuffer.wrap(
                    emulator.getBackend().mem_read(metasecModule.base + 0x3deda8L, 8))
                    .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                if (dtPtr2 != 0) {
                    long h40 = java.nio.ByteBuffer.wrap(
                        emulator.getBackend().mem_read(dtPtr2 + 40L * 8, 8))
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                    System.out.printf("[init后] handler[40]=0x%x%n", h40);
                    if (h40 != 0) {
                        byte[] hraw = emulator.getBackend().mem_read(h40, 128);
                        System.out.println("[init后] handler obj 128B: " + bytesToHex(hraw));
                    }
                }                
                System.out.println("================================\n");
            } catch(Exception e) {
                System.out.println("dispatch table dump 异常: " + e);
            }

        } catch (Exception e) { System.out.println("初始化异常: " + e.getMessage()); }

        // ===== Fix 1：反汇编 0x270140 =====
        System.out.println("\n--- 反汇编 0x270140 ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x270140L, 0x30, 0, null);
        System.out.println("---\n");

        // ===== 检查 ttcrypto 字节并反汇编 0x32a1f0 和 0x16c9d0 =====
        byte[] ttCode = emulator.getBackend().mem_read(ttCryptoModule.base + 0x231c80L, 16);
        System.out.println("[ttcrypto+0x231c80 字节] " + bytesToHex(ttCode));

        System.out.println("\n--- 反汇编 0x32a1f0 (初始化/检查函数) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x32a1f0L, 0x40, 0, null);
        System.out.println("---\n");

        System.out.println("\n--- 反汇编 0x16c9d0 (真正的签名函数) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x16c9d0L, 0x80, 0, null);
        System.out.println("---\n");

        // Hook 0x32a1f0 内部的检查点
        final long check1Addr = metasecModule.base + 0x32a210L;
        final long check2Addr = metasecModule.base + 0x32a218L;
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                try {
                    if (address == check1Addr) {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        System.out.println("[0x32a210] 0x347fd0 返回: x0=" + x0 + " (" + (x0 != 0 ? "通过" : "失败") + ")");
                    } else if (address == check2Addr) {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        System.out.println("[0x32a218] 0x35d80c 返回: x0=" + x0 + " (" + (x0 != 0 ? "通过" : "失败") + ")");
                    }
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, check1Addr, check2Addr + 4, null);

        // 监控假密钥是否被读取
        emulator.getBackend().hook_add_new(
            new com.github.unidbg.arm.backend.ReadHook() {
                @Override
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    if (capturedFakeKeyAddr[0] == 0 || address < capturedFakeKeyAddr[0] || address >= capturedFakeKeyAddr[0] + 64) {
                        return;
                    }
                    try {
                        long pc = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_PC)).longValue();
                        System.out.printf("[fakeKey被读!] 地址偏移+%d  PC_off=0x%x%n",
                            (int)(address - capturedFakeKeyAddr[0]), pc - metasecModule.base);
                    } catch(Exception e) {}
                }
                @Override public void detach(){}
                @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, 0x12200000L, 0x12400000L, null);

        // ===== 读取 PLT→GOT 真实函数地址 =====
        long gotAddr = metasecModule.base + 0x375218L;
        long realFn = ByteBuffer.wrap(emulator.getBackend().mem_read(gotAddr, 8))
            .order(ByteOrder.LITTLE_ENDIAN).getLong();
        System.out.printf("PLT→GOT[0x218] 真实函数地址 = 0x%x (ttcrypto偏移=0x%x)%n",
            realFn, realFn - ttCryptoModule.base);
        System.out.println("--- 反汇编真实签名函数前128字节 ---");
        emulator.printAssemble(System.out, realFn, 0x80, 0, null);
        System.out.println("---");
        
        // ========== 新增的三个关键 hook（根据分析结果） ==========
        // 必须在 Argus 调用之前添加
        System.out.println("\n--- 反汇编 0x126c7f10 (metasec+0x347f10) ---");
        emulator.printAssemble(System.out, metasecModule.base + 0x347f10L, 0x80, 0, null);
        System.out.println("---");
        // ===== Hook 1：0x26c9d0 入口 — 签名计算函数 =====
        final long computeFn = metasecModule.base + 0x26c9d0L;
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            boolean fired = false;
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address != computeFn) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long x2 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2).longValue();
                    long x3 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X3).longValue();
                    long x8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue();
                    long lr = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR).longValue();
                    System.out.printf("%n===== [0x26c9d0] 签名计算函数 =====%n");
                    System.out.printf("  x0=0x%x  x1=0x%x  x2=0x%x  x3=0x%x  x8=0x%x  LR_off=0x%x%n",
                        x0, x1, x2, x3, x8, lr - metasecModule.base);
                    for (long[] rp : new long[][]{{x0,'x','0'},{x1,'x','1'},{x2,'x','2'},{x3,'x','3'},{x8,'x','8'}}) {
                        if (rp[0] > 0x11000000L && rp[0] < 0x20000000L) {
                            try {
                                System.out.println("  [" + (char)rp[1] + (char)rp[2] + " dump64] " + bytesToHex(backend.mem_read(rp[0], 64)));
                            } catch(Exception ex) {}
                        }
                    }
                    byte[] x0struct = backend.mem_read(x0, 32);
                    System.out.println("  [x0 struct 32B] " + bytesToHex(x0struct));
                    long flags = ByteBuffer.wrap(x0struct, 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
                    long typeId = ByteBuffer.wrap(x0struct, 16, 8).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    int typeIdx = (int)((typeId >> 4) & 0xFF);
                    System.out.printf("  flags(+8)=0x%x  type_id(+0x10)=0x%x  => table_index=%d%n", flags, typeId, typeIdx);
                    byte[] dpRaw = backend.mem_read(metasecModule.base + 0x3deda8L, 8);
                    long dispPtr = ByteBuffer.wrap(dpRaw).order(ByteOrder.LITTLE_ENDIAN).getLong();
                    System.out.printf("  dispatch_table ptr=0x%x%n", dispPtr);
                    if (dispPtr != 0 && typeIdx >= 0 && typeIdx < 256) {
                        byte[] slotRaw = backend.mem_read(dispPtr + typeIdx * 8L, 8);
                        long slot = ByteBuffer.wrap(slotRaw).order(ByteOrder.LITTLE_ENDIAN).getLong();
                        System.out.printf("  handler[%d]=0x%x (%s)%n", typeIdx, slot, slot==0?"NULL=失败!":"有效!");
                    }
                    byte[] glb = backend.mem_read(metasecModule.base + 0x3d1570L, 32);
                    System.out.println("  [metasec+0x3d1570 dump32] " + bytesToHex(glb));
                    if (!fired) {
                        fired = true;
                        System.out.println("  [反汇编 0x26c9d0 前128B]");
                        emulator.printAssemble(System.out, computeFn, 0x80, 0, null);
                    }
                    System.out.println("==============================\n");
                } catch(Exception e) { System.out.println("[0x26c9d0 hook异常] " + e); }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, computeFn, computeFn + 4, null);

        // ===== 验证 0x26c9d0 内部实际执行路径 =====
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x26c9d8L) return;
                long w8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue() & 0xFFFFFFFFL;
                long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                System.out.printf("[0x26c9d8] ldr后 w8=0x%x (cbz %s走签名路径)  x0=0x%x%n",
                    w8, w8==0?"会":"不会", x0);
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26c9d8L, metasecModule.base + 0x26c9dcL, null);

        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x26c9fcL) return;
                long x8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue();
                long x9 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X9).longValue();
                System.out.printf("[0x26c9fc] cbz前 x8(handler)=0x%x  x9(table)=0x%x  -> %s%n",
                    x8, x9, x8==0?"走ret(失败!)":"走0x126c7f10(成功!)");
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26c9fcL, metasecModule.base + 0x26ca00L, null);

        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x26ca04L) return;
                long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                System.out.printf("[0x26ca04] ✅ b #0x126c7f10 即将执行! x0=0x%x%n", x0);
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26ca04L, metasecModule.base + 0x26ca08L, null);

        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x26c9e4L) return;
                long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                System.out.printf("[0x26c9e4] ❌ early ret! x0=0x%x%n", x0);
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x26c9e4L, metasecModule.base + 0x26c9e8L, null);

        // ===== Hook 2：0x1625f4 前的寄存器快照 — 传给 0x26c9d0 的参数 =====
        // 注意：此 hook 与后面注入 hook 共用同一地址，但只用于打印，不修改任何内容
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off != 0x1625f4L) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long x2 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2).longValue();
                    long x3 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X3).longValue();
                    long x4 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X4).longValue();
                    long x5 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X5).longValue();
                    System.out.printf("[0x1625f4] 调用 0x26c9d0 前: x0=0x%x x1=0x%x x2=0x%x x3=0x%x x4=0x%x x5=0x%x%n",
                        x0, x1, x2, x3, x4, x5);
                    if (x3 > 0x12000000L && x3 < 0x14000000L) {
                        byte[] d3 = backend.mem_read(x3, 64);
                        System.out.println("  [x3 dump64] " + bytesToHex(d3));
                    }
                    if (x4 > 0x12000000L && x4 < 0x14000000L) {
                        byte[] d4 = backend.mem_read(x4, 64);
                        System.out.println("  [x4 dump64] " + bytesToHex(d4));
                    }
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x1625f4L, metasecModule.base + 0x1625f8L, null);

        // ---- 注入假签名到结果容器（不拦截，不修改指令） ----
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            boolean fired = false;
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object u) {
                if (address - metasecModule.base != 0x1625f4L || fired) return;
                fired = true;
                try {
                    long x20 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20).longValue();
                    long x3  = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X3).longValue();
                    long x4  = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X4).longValue();
                    System.out.printf("[0x1625f4注入] x20=0x%x x3=0x%x x4=0x%x%n", x20, x3, x4);
                    if (x20 > 0x10000000L && x20 < 0xf0000000L) {
                        byte[] containerBytes = backend.mem_read(x20 + 8, 8);
                        long resultContainer = ByteBuffer.wrap(containerBytes)
                            .order(ByteOrder.LITTLE_ENDIAN).getLong();
                        System.out.printf("  resultContainer=0x%x%n", resultContainer);
                        if (resultContainer > 0x12000000L && resultContainer < 0x14000000L) {
                            byte[] fakeSign = new byte[256];
                            Arrays.fill(fakeSign, (byte)0x42);
                            backend.mem_write(resultContainer + 8, fakeSign);
                            System.out.println("  [写入假签名到 resultContainer+8]");
                        }
                    }
                } catch(Exception e) { System.out.println("[0x1625f4注入异常] " + e); }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x1625f4L, metasecModule.base + 0x1625f8L, null);

        // ---- 0x162618 ret 处 dump 返回状态和结果容器 ----
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object u) {
                if (address - metasecModule.base != 0x162618L) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x20 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20).longValue();
                    System.out.printf("[0x162618 ret] x0=0x%x (0=rdlock成功) x20=0x%x%n", x0, x20);
                    if (x20 > 0x12000000L && x20 < 0xf0000000L) {
                        byte[] x20c = backend.mem_read(x20, 24);
                        System.out.println("  [x20 dump24] " + bytesToHex(x20c));
                        long rc = ByteBuffer.wrap(x20c, 8, 8)
                            .order(ByteOrder.LITTLE_ENDIAN).getLong();
                        if (rc > 0x12000000L && rc < 0x14000000L) {
                            System.out.println("  [x20+8=resultContainer dump32] " +
                                bytesToHex(backend.mem_read(rc, 32)));
                        }
                    }
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x162618L, metasecModule.base + 0x16261cL, null);

        // ===== 签名实际执行函数 0x2e7f10 =====
        final long signExecFn = metasecModule.base + 0x347f10L;
        System.out.println("签名执行函数地址 = 0x" + Long.toHexString(signExecFn));
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            boolean fired = false;
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address != signExecFn) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long x2 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2).longValue();
                    long x3 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X3).longValue();
                    long lr = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR).longValue();
                    System.out.printf("%n========== [0x2e7f10] 签名执行函数 ==========%n");
                    System.out.printf("  x0=0x%x  x1=0x%x  x2=0x%x  x3=0x%x  LR_off=0x%x%n",
                        x0, x1, x2, x3, lr - metasecModule.base);

                    if (x0 > 0x12000000L && x0 < 0x14000000L) {
                        long handlerBase = x0 - 8;
                        byte[] hd = backend.mem_read(handlerBase, 128);
                        System.out.println("  [handler obj dump 128B from " + Long.toHexString(handlerBase) + "] " + bytesToHex(hd));
                        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(hd).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                        long vtbl = bb.getLong(); long f8 = bb.getLong(); long f10 = bb.getLong(); long f18 = bb.getLong();
                        System.out.printf("  handler[+0]=0x%x  [+8]=0x%x  [+0x10]=0x%x  [+0x18]=0x%x%n", vtbl, f8, f10, f18);
                        if (f8 > 0x12000000L && f8 < 0x14000000L) {
                            byte[] keyDump = backend.mem_read(f8, 64);
                            System.out.println("  [handler[+8] ptr dump 64B] " + bytesToHex(keyDump));
                        } else {
                            System.out.println("  !! handler[+8] 为 0 或无效 → SM4 密钥/上下文缺失 !!");
                        }
                    }

                    if (x1 > 0x12000000L && x1 < 0x14000000L) {
                        byte[] s = backend.mem_read(x1, 32);
                        System.out.println("  [x1 slice dump 32B] " + bytesToHex(s));
                    }

                    if (!fired) {
                        fired = true;
                        System.out.println("  [反汇编 0x2e7f10 前128B]");
                        emulator.printAssemble(System.out, signExecFn, 0x80, 0, null);
                    }
                    System.out.println("==========================================\n");
                } catch(Exception e) { System.out.println("[0x2e7f10 hook异常] " + e); }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, signExecFn, signExecFn + 0x100L, null);

        // ========== 新增 hook 结束 ==========

        // ===== 新增：Argus 签名区 0x163e0c =====
        final long argusSignFn = metasecModule.base + 0x163e0cL;
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            boolean fired = false;
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address != argusSignFn || fired) return;
                fired = true;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long x2 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2).longValue();
                    long lr = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR).longValue();
                    System.out.printf("%n===== [0x163e0c] Argus签名区入口 =====%n");
                    System.out.printf("  x0=0x%x  x1=0x%x  x2=0x%x  LR_off=0x%x%n",
                        x0, x1, x2, lr - metasecModule.base);
                    for (long[] rp : new long[][]{{x0,0},{x1,1},{x2,2}}) {
                        if (rp[0] > 0x12000000L && rp[0] < 0x14000000L) {
                            byte[] d = backend.mem_read(rp[0], 64);
                            System.out.println("  [x" + rp[1] + " dump64] " + bytesToHex(d));
                        }
                    }
                    emulator.printAssemble(System.out, argusSignFn, 0x80, 0, null);
                    System.out.println("==========================================\n");
                } catch(Exception e) { System.out.println("[0x163e0c hook异常] " + e); }
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, argusSignFn, argusSignFn + 4, null);

        // ===== 新增：Argus 调用 0x26c9d0 前 sp dump =====
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x163e2cL) return;
                try {
                    long sp = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_SP).longValue();
                    byte[] spDump = backend.mem_read(sp, 32);
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    System.out.printf("[0x163e2c] 调用0x26c9d0前: sp=0x%x  x1=0x%x%n  [sp+0 32B] %s%n",
                        sp, x1, bytesToHex(spDump));
                    long flags = java.nio.ByteBuffer.wrap(spDump, 8, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt() & 0xFFFFFFFFL;
                    long typeId = java.nio.ByteBuffer.wrap(spDump, 16, 8).order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                    System.out.printf("  [sp+8]=0x%x(flags)  [sp+0x10]=0x%x(type_id)  index=%d%n",
                        flags, typeId, (int)((typeId>>4)&0xFF));
                        
                    long dtPtr = java.nio.ByteBuffer.wrap(backend.mem_read(metasecModule.base + 0x3deda8L, 8))
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                    if (dtPtr != 0) {
                        long h135 = java.nio.ByteBuffer.wrap(backend.mem_read(dtPtr + 135L * 8, 8))
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                        long h40  = java.nio.ByteBuffer.wrap(backend.mem_read(dtPtr + 40L  * 8, 8))
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                        System.out.printf("  dispatch_table[135(Argus)]=0x%x  [40(Ladon)]=0x%x%n", h135, h40);
                        if (h135 != 0L) {
                            byte[] hd = backend.mem_read(h135, 128);
                            System.out.println("  [Argus handler@0x" + Long.toHexString(h135) + " 128B] " + bytesToHex(hd));
                            java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(hd).order(java.nio.ByteOrder.LITTLE_ENDIAN);
                            long vtbl = bb.getLong();
                            long f8   = bb.getLong();
                            long f10  = bb.getLong();
                            long f18  = bb.getLong();
                            System.out.printf("  vtbl=0x%x  [+8]=0x%x  [+0x10]=0x%x  [+0x18]=0x%x%n", vtbl, f8, f10, f18);
                        
                            // 清除 bits 14-15（写锁位）
                            byte[] stateBytes = backend.mem_read(h135 + 0x08L, 2);
                            int origState = (stateBytes[0] & 0xFF) | ((stateBytes[1] & 0xFF) << 8);
                            int newState = origState & 0x3FFF;
                            backend.mem_write(h135 + 0x08L,
                                new byte[]{(byte)(newState & 0xFF), (byte)(newState >> 8)});
                            System.out.printf("  [rwlock修复] 0x%04x → 0x%04x (只清写锁位)%n", origState, newState);
                        
                            // 注入 SM4 密钥指针到 +0x10
                            long sm4KeyAddr = emulator.getMemory().malloc(32, true).getPointer().peer;
                            byte[] sm4Key = new byte[]{
                                0x01, 0x23, 0x45, 0x67, (byte)0x89, (byte)0xab, (byte)0xcd, (byte)0xef,
                                (byte)0xfe, (byte)0xdc, (byte)0xba, (byte)0x98, 0x76, 0x54, 0x32, 0x10
                            };
                            backend.mem_write(sm4KeyAddr, sm4Key);
                            byte[] sm4Ptr = ByteBuffer.allocate(8)
                                .order(ByteOrder.LITTLE_ENDIAN).putLong(sm4KeyAddr).array();
                            backend.mem_write(h135 + 0x10L, sm4Ptr);

                            long sigHandlerAddr = 0x129e9870L;
                            byte[] shPtr = ByteBuffer.allocate(8)
                                .order(ByteOrder.LITTLE_ENDIAN).putLong(sigHandlerAddr).array();
                            backend.mem_write(h135 + 0x10L, shPtr);
                            
                            capturedFakeKeyAddr[0] = sm4KeyAddr;
                            System.out.printf("  [SM4注入] key_ptr=0x%x @ +0x10%n", sm4KeyAddr);
                        
                            final long captHdl = h135;
                            emulator.getBackend().hook_add_new(
                                new com.github.unidbg.arm.backend.ReadHook() {
                                    @Override
                                    public void hook(com.github.unidbg.arm.backend.Backend b, long address, int size, Object u) {
                                        long off = address - captHdl;
                                        if (off < 0 || off >= 512) return;
                                        try {
                                            long pc = ((Number)b.reg_read(unicorn.Arm64Const.UC_ARM64_REG_PC)).longValue();
                                            long libcOff = pc - 0x12190000L;
                                            System.out.printf("[handler读取] +0x%02x (size=%d) libc+0x%x%n", off, size, libcOff);
                                        } catch (Exception e) {}
                                    }
                                    @Override public void detach(){}
                                    @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
                                }, captHdl, captHdl + 512L, null);
                        
                            final long captKey = sm4KeyAddr;
                            emulator.getBackend().hook_add_new(
                                new com.github.unidbg.arm.backend.ReadHook() {
                                    @Override
                                    public void hook(com.github.unidbg.arm.backend.Backend b, long address, int size, Object u) {
                                        long off = address - captKey;
                                        if (off < 0 || off >= 64) return;
                                        try {
                                            long pc = ((Number)b.reg_read(unicorn.Arm64Const.UC_ARM64_REG_PC)).longValue();
                                            long libcOff = pc - 0x12190000L;
                                            System.out.printf("[SM4key读取] +0x%02x (size=%d) libc+0x%x%n", off, size, libcOff);
                                        } catch (Exception e) {}
                                    }
                                    @Override public void detach(){}
                                    @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
                                }, captKey, captKey + 64L, null);
                        }
                    }                                                
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x163e2cL, metasecModule.base + 0x163e30L, null);    
        
        // ---- 追踪 ctx 对象（完整版） ----
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                long off = address - metasecModule.base;
                if (off != 0x163e30L) return;
                try {
                    long x0  = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x19 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X19).longValue();
                    long x20 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20).longValue();
                    System.out.printf("[0x163e30] x0(签名返回)=0x%x  x19=0x%x  x20=0x%x%n", x0, x19, x20);
                    if (x20 > 0x10000000L && x20 < 0xf0000000L) {
                        byte[] x20d = backend.mem_read(x20, 32);
                        System.out.println("  [x20 dump32] " + bytesToHex(x20d));
                        long ctxPtr = java.nio.ByteBuffer.wrap(x20d, 8, 8)
                            .order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong();
                        System.out.printf("  [x20+8]=ctx=0x%x%n", ctxPtr);
                        if (ctxPtr > 0x12000000L && ctxPtr < 0x14000000L) {
                            byte[] ctxd = backend.mem_read(ctxPtr, 64);
                            System.out.println("  [ctx dump64] " + bytesToHex(ctxd));
                        }
                    }
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x163e30L, metasecModule.base + 0x163e34L, null);

        // ---- Ladon 新路径 0x32d3a0 hook ----
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend b, long address, int size, Object u) {
                if (address - metasecModule.base != 0x32d3a0L) return;
                try {
                    long x0 = b.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = b.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long lr = b.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR).longValue();
                    System.out.printf("[0x32d3a0] Ladon新路径! x0=0x%x x1=0x%x LR_off=0x%x%n",
                        x0, x1, lr - metasecModule.base);
                    emulator.printAssemble(System.out, metasecModule.base + 0x32d3a0L, 0x40, 0, null);
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x32d3a0L, metasecModule.base + 0x32d3a4L, null);

        // ===== 1. Argus: 0x163e30 = bl 0x26c9d0 的返回地址（也是 0x126c7f10 的 ret 目标）=====
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x163e30L) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    System.out.printf("[0x163e30] Argus签名返回: x0=0x%x (%s)%n",
                        x0, x0 == 0 ? "NULL=失败" : "非空→签名有结果");
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x163e30L, metasecModule.base + 0x163e34L, null);

        // ---- 监控 0x163e50 ret 时的 x0 ----
        emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
            public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                if (address - metasecModule.base != 0x163e50L) return;
                try {
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    System.out.printf("[0x163e50] ret x0=0x%x (%s)%n", x0, x0==0 ? "null → 失败" : "非空 → 继续");
                } catch(Exception e) {}
            }
            public void detach(){}
            public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
        }, metasecModule.base + 0x163e50L, metasecModule.base + 0x163e54L, null);

        // 3. X-Argus (0x3000001)
        System.out.println("正在生成 X-Argus...");
        System.out.println(">>> ARGUS_TRACE_START <<<");
        try {
            long argusParamAddr = emulator.getMemory().malloc(256, true).getPointer().peer;
            long argusSecondaryAddr = emulator.getMemory().malloc(64, true).getPointer().peer;
            byte[] fakeDeviceData = "860123456789012".getBytes();
            emulator.getBackend().mem_write(argusSecondaryAddr, fakeDeviceData);
            emulator.getBackend().mem_write(argusSecondaryAddr + fakeDeviceData.length, new byte[]{0});
            byte[] ptrBytes2 = new byte[8];
            for (int b = 0; b < 8; b++) ptrBytes2[b] = (byte)(argusSecondaryAddr >> (b*8));
            emulator.getBackend().mem_write(argusParamAddr + 0x18, ptrBytes2);
            System.out.println("  [调试] argus参数结构体地址=0x" + Long.toHexString(argusParamAddr)
                + " , [+0x18]指向次级buffer=0x" + Long.toHexString(argusSecondaryAddr) + "(已填充非零数据)");

            final long signFnEntry = metasecModule.base + 0x26f258L;
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    if (address != signFnEntry) return;
                    long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                    long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                    long x20 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X20).longValue();
                    System.out.printf("[0x26f258] 签名函数入口: x0=0x%x x1=0x%x x20=0x%x%n", x0, x1, x20);
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, signFnEntry, signFnEntry + 4, null);

            final long branchTarget = metasecModule.base + 0x1625a4L;
            final boolean[] firedBranch = {false};
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    try {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        long x1 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1).longValue();
                        long x8 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8).longValue();
                        System.out.printf("[0x1625a4] 跳转目标入口: x0=0x%x x1=0x%x x8=0x%x%n", x0, x1, x8);
                        if (x1 > 0x10000000L && x1 < 0x40000000L) {
                            byte[] d = backend.mem_read(x1, 128);
                            System.out.println("  [x1 ptr dump 128B] " + bytesToHex(d));
                        }
                        if (!firedBranch[0]) {
                            firedBranch[0] = true;
                            System.out.println("  [反汇编 0x1625a4 前128B]");
                            emulator.printAssemble(System.out, branchTarget, 0x80, 0, null);
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, branchTarget, branchTarget + 4, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    try {
                        byte[] inst = backend.mem_read(address, 4);
                        if ((inst[3] & 0xFC) == 0x94) {
                            int imm = ((inst[3] & 0x03) << 24) | ((inst[2] & 0xFF) << 16) | ((inst[1] & 0xFF) << 8) | (inst[0] & 0xFF);
                            if ((imm & 0x02000000) != 0) imm |= 0xFC000000;
                            long target = address + (imm * 4);
                            System.out.println("[0x1625a4内部:0x" + Long.toHexString(off) + "] BL 0x" + Long.toHexString(target - metasecModule.base));
                        } else if ((inst[3] & 0xFF) == 0xD6 && (inst[2] & 0xFF) == 0x3F) {
                            long reg = ((inst[1] & 0x03) << 3) | ((inst[0] >> 5) & 0x07);
                            long val = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0 + (int)reg).longValue();
                            System.out.println("[0x1625a4内部:0x" + Long.toHexString(off) + "] BLR X" + reg + "=0x" + Long.toHexString(val));
                        } else if (inst[0] == (byte)0xC0 && inst[1] == 0x03 && inst[2] == 0x5F && inst[3] == (byte)0xD6) {
                            long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                            System.out.println("[0x1625a4内部:0x" + Long.toHexString(off) + "] RET, x0=0x" + Long.toHexString(x0));
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, branchTarget, branchTarget + 0x200, null);

            final long checkAddr = metasecModule.base + 0x270168L;
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    try {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        System.out.println("[0x270168] 检查点: cbz x0, #0x270180, 当前 x0=0x" + Long.toHexString(x0));
                        if (x0 > 0x10000000L && x0 < 0x40000000L) {
                            byte[] d = backend.mem_read(x0, 64);
                            System.out.println("  [x0 dump 64B] " + bytesToHex(d));
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, checkAddr, checkAddr + 4, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    try {
                        long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                        long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                        long x8 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                        long lr = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                        System.out.println("[逐条:0x" + Long.toHexString(off) + "] x0=0x" + Long.toHexString(x0)
                            + " x1=0x" + Long.toHexString(x1) + " x8=0x" + Long.toHexString(x8)
                            + " LR=0x" + Long.toHexString(lr - metasecModule.base));
                    } catch(Exception e) {
                        System.out.println("[逐条:0x" + Long.toHexString(off) + "] 寄存器读取异常: " + e);
                    }
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x270120L, metasecModule.base + 0x270250L, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    try {
                        long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                        long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                        long x8 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                        long lr = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_LR)).longValue();
                        System.out.println("[编排:0x" + Long.toHexString(off) + "] x0=0x" + Long.toHexString(x0)
                            + " x1=0x" + Long.toHexString(x1) + " x8=0x" + Long.toHexString(x8)
                            + " LR=0x" + Long.toHexString(lr - metasecModule.base));
                    } catch(Exception e) {
                        System.out.println("[编排:0x" + Long.toHexString(off) + "] 寄存器读取异常: " + e);
                    }
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x26ff80L, metasecModule.base + 0x270000L, null);

            final long[] lastGoodByteArrayHandle = {0};
            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    try {
                        long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                        if (x0 != 0) {
                            lastGoodByteArrayHandle[0] = x0;
                            System.out.println("  [handle捕获] getBytes()返回正确handle=0x" + Long.toHexString(x0));
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x26f960L, metasecModule.base + 0x26f960L + 4, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    try {
                        if (lastGoodByteArrayHandle[0] == 0) return;
                        long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                        if (x1 != lastGoodByteArrayHandle[0]) {
                            System.out.println("  [handle修正] 0x" + Long.toHexString(address - metasecModule.base)
                                + " x1从0x" + Long.toHexString(x1) + "修正为0x" + Long.toHexString(lastGoodByteArrayHandle[0]));
                            backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X1, lastGoodByteArrayHandle[0]);
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x2700e0L, metasecModule.base + 0x2700e4L, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    try {
                        if (lastGoodByteArrayHandle[0] == 0) return;
                        long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                        if (x1 != lastGoodByteArrayHandle[0]) {
                            System.out.println("  [handle修正] 0x" + Long.toHexString(address - metasecModule.base)
                                + " x1从0x" + Long.toHexString(x1) + "修正为0x" + Long.toHexString(lastGoodByteArrayHandle[0]));
                            backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X1, lastGoodByteArrayHandle[0]);
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x270120L, metasecModule.base + 0x270124L, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    long off = address - metasecModule.base;
                    try {
                        long x0 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0)).longValue();
                        long x1 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X1)).longValue();
                        long x2 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X2)).longValue();
                        long x3 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X3)).longValue();
                        long x8 = ((Number)backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X8)).longValue();
                        System.out.println("[Release猜测验证:0x" + Long.toHexString(off) + "] x0(env)=0x" + Long.toHexString(x0)
                            + " x1(array)=0x" + Long.toHexString(x1) + " x2(ptr)=0x" + Long.toHexString(x2)
                            + " x3(mode)=0x" + Long.toHexString(x3) + " x8(fn)=0x" + Long.toHexString(x8));
                        byte[] inst = backend.mem_read(address, 4);
                        boolean isBlr = (inst[3] & 0xFF) == 0xD6 && (inst[2] & 0xFF) == 0x3F;
                        if (isBlr && lastGoodByteArrayHandle[0] != 0 && x1 != lastGoodByteArrayHandle[0]) {
                            System.out.println("  [Release handle修正] x1: 0x" + Long.toHexString(x1)
                                + " -> 0x" + Long.toHexString(lastGoodByteArrayHandle[0]));
                            backend.reg_write(unicorn.Arm64Const.UC_ARM64_REG_X1, lastGoodByteArrayHandle[0]);
                        }
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x26ffa0L, metasecModule.base + 0x26ffe0L, null);

            final long argusBase = argusParamAddr;
            final java.util.Set<Long> argusReadOffsets = new java.util.TreeSet<>();
            final boolean[] argusHookActive = {true};
            emulator.getBackend().hook_add_new(
                new com.github.unidbg.arm.backend.ReadHook() {
                    @Override
                    public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                        if (!argusHookActive[0]) return;
                        long delta = address - argusBase;
                        if (delta >= 0 && delta < 256) argusReadOffsets.add(delta);
                    }
                    @Override public void detach(){}
                    @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
                }, argusParamAddr, argusParamAddr + 256, null);

            printResult(y2Class.callStaticJniMethodObject(emulator, "a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;", 0x3000001, 0, argusParamAddr, url, null));
            argusHookActive[0] = false;
            System.out.println("  [指令计数] Argus(0x3000001) 执行了 " + insnCount + " 条指令");
            System.out.println("  [结构体读取偏移] " + argusReadOffsets);
            System.out.println("  [Argus末尾32条PC] " + dumpRecentPC());
        } catch (Exception e) { System.out.println("Argus 生成异常: " + e.getMessage()); e.printStackTrace(); }
        System.out.println(">>> ARGUS_TRACE_END <<<");

        // 4. X-Ladon (0x4000002)
        System.out.println("正在生成 X-Ladon...");
        System.out.println(">>> LADON_TRACE_START <<<");
        try {
            long ladonParamAddr = emulator.getMemory().malloc(256, true).getPointer().peer;
            long ladonSecondaryAddr = emulator.getMemory().malloc(64, true).getPointer().peer;
            byte[] lp = new byte[8];
            for (int b = 0; b < 8; b++) lp[b] = (byte)(ladonSecondaryAddr >> (b*8));
            emulator.getBackend().mem_write(ladonParamAddr + 0x18, lp);
            System.out.println("  [调试] ladon参数结构体地址=0x" + Long.toHexString(ladonParamAddr)
                + " , [+0x18]指向次级buffer=0x" + Long.toHexString(ladonSecondaryAddr));

            final long ladonBase = ladonParamAddr;
            final java.util.Set<Long> ladonReadOffsets = new java.util.TreeSet<>();
            final boolean[] ladonHookActive = {true};
            emulator.getBackend().hook_add_new(
                new com.github.unidbg.arm.backend.ReadHook() {
                    @Override
                    public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                        if (!ladonHookActive[0]) return;
                        long delta = address - ladonBase;
                        if (delta >= 0 && delta < 256) ladonReadOffsets.add(delta);
                    }
                    @Override public void detach(){}
                    @Override public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
                }, ladonParamAddr, ladonParamAddr + 256, null);

            emulator.getBackend().hook_add_new(new com.github.unidbg.arm.backend.CodeHook() {
                public void hook(com.github.unidbg.arm.backend.Backend backend, long address, int size, Object user) {
                    if (address - metasecModule.base != 0x16aa4cL) return;
                    try {
                        long x0 = backend.reg_read(unicorn.Arm64Const.UC_ARM64_REG_X0).longValue();
                        System.out.printf("[0x16aa4c] Ladon签名返回: x0=0x%x (%s)%n",
                            x0, x0 == 0 ? "NULL=失败" : "非空→签名有结果");
                    } catch(Exception e) {}
                }
                public void detach(){}
                public void onAttach(com.github.unidbg.arm.backend.UnHook u){}
            }, metasecModule.base + 0x16aa4cL, metasecModule.base + 0x16aa50L, null);

            printResult(y2Class.callStaticJniMethodObject(emulator, "a(IIJLjava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;", 0x4000002, 0, ladonParamAddr, url, null));
            ladonHookActive[0] = false;
            System.out.println("  [Ladon结构体读取偏移] " + ladonReadOffsets);
            System.out.println("  [指令计数] Ladon(0x4000002) 执行了 " + insnCount + " 条指令");
            System.out.println("  [Ladon末尾32条PC] " + dumpRecentPC());
        } catch (Exception e) { System.out.println("Ladon 生成异常: " + e.getMessage()); }
        System.out.println(">>> LADON_TRACE_END <<<");
    }

    private void printResult(DvmObject r) {
        if (r != null) {
            Object val = r.getValue();
            if (val instanceof String[]) {
                String[] arr = (String[]) val;
                for (int i = 0; i + 1 < arr.length; i += 2) {
                    System.out.println("  " + arr[i] + ": " + arr[i+1]);
                }
            } else {
                System.out.println("  结果: " + val);
            }
        } else {
            System.out.println("  结果为空");
        }
    }

    @Override
    public DvmObject<?> callObjectMethodV(BaseVM vm, DvmObject<?> dvmObject,
                                           String signature, VaList vaList) {
        System.out.println("[JNI-obj] " + signature + " on " + dvmObject);
    
        if ("java/lang/String->getBytes(Ljava/lang/String;)[B".equals(signature)
                || "java/lang/String->getBytes()[B".equals(signature)) {
            try {
                String str = (String) dvmObject.getValue();
                String charset = "UTF-8";
                if ("java/lang/String->getBytes(Ljava/lang/String;)[B".equals(signature)) {
                    DvmObject<?> charsetObj = vaList.getObjectArg(0);
                    if (charsetObj != null && charsetObj.getValue() instanceof String) {
                        charset = (String) charsetObj.getValue();
                    }
                }
                byte[] bytes = str.getBytes(charset);
                System.out.println("[callObjectMethodV] getBytes(" + charset + ") => " + bytes.length + " bytes");
                return new com.github.unidbg.linux.android.dvm.array.ByteArray(vm, bytes);
            } catch (Exception e) {
                System.out.println("[callObjectMethodV] getBytes异常: " + e);
            }
        }
    
        System.out.println("  ⚠️ 未处理的对象方法: " + signature);
        if (signature.contains("toString")) {
            return new DvmString(vm, "");
        }
        if (signature.contains("length")) {
            return new DvmInteger(vm, 0);
        }
        return super.callObjectMethodV(vm, dvmObject, signature, vaList);
    }

    @Override
    public DvmObject<?> callStaticObjectMethodV(BaseVM vm, DvmClass dvmClass,
                                                 String signature, VaList vaList) {
        System.out.println("[JNI-static] " + signature);
    
        try {
            for (int i = 0; i < 4; i++) {
                System.out.printf("  arg%d(int)=0x%x%n", i, vaList.getIntArg(i));
            }
        } catch (Exception e) {}
    
        if (signature.contains("MS->b")) {
            int subCmd = vaList.getIntArg(0);
            System.out.printf("  [MS.b] subCmd=0x%x%n", subCmd);
            switch (subCmd) {
                case 0x1000000e:
                    return new DvmLong(vm, fakeContextAddr);
                default:
                    System.out.println("  [MS.b] ⚠️ 未处理的 subCmd=0x" + Integer.toHexString(subCmd) + "，返回 0");
                    return new DvmLong(vm, 0L);
            }
        }
    
        if (signature.contains("Integer->valueOf")) {
            int val = vaList.getIntArg(0);
            if (val == -1) val = 0;
            System.out.printf("  [Integer] valueOf(%d)%n", val);
            return new DvmInteger(vm, val);
        }
    
        System.out.println("  ⚠️ 未处理的静态方法: " + signature);
        return super.callStaticObjectMethodV(vm, dvmClass, signature, vaList);
    }

    @Override
    public long callLongMethodV(BaseVM vm, DvmObject<?> dvmObject, String signature, VaList vaList) {
        if (signature.contains("longValue")) {
            Object val = dvmObject.getValue();
            if (val instanceof Long) return (Long) val;
            if (val instanceof Integer) return ((Integer) val).longValue();
        }
        return super.callLongMethodV(vm, dvmObject, signature, vaList);
    }

    static class DvmLong extends DvmObject<Long> {
        DvmLong(BaseVM vm, long val) { super(vm.resolveClass("java/lang/Long"), val); }
    }
    static class DvmInteger extends DvmObject<Integer> {
        DvmInteger(BaseVM vm, int val) { super(vm.resolveClass("java/lang/Integer"), val); }
    }
    static class DvmString extends DvmObject<String> {
        DvmString(BaseVM vm, String val) {
            super(vm.resolveClass("java/lang/String"), val);
        }
    }

    public static void main(String[] args) {
        TomatoSigner signer = new TomatoSigner();
        signer.testSign();
    }
}