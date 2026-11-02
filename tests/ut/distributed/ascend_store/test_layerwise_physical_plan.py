"""Model-independent coverage for declared hybrid backing packs."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest
import torch
from vllm.v1.kv_cache_interface import KVCacheConfig, KVCacheGroupSpec, KVCacheTensor, MambaSpec, MLAAttentionSpec

from vllm_ascend.distributed.kv_transfer.kv_pool.ascend_store.layerwise.layerwise_cache_layout import (
    LayerwiseCacheTopology,
    apply_layerwise_kv_cache_plan,
    get_layerwise_reuse_layout,
)


def make_plan(buffers=1, independent=None):
    attention = MLAAttentionSpec(block_size=16, num_kv_heads=1, head_size=16, dtype=torch.bfloat16)
    recurrent = MambaSpec(
        block_size=16,
        shapes=((3, 16), (1, 4, 4)),
        dtypes=(torch.bfloat16, torch.float32),
        mamba_cache_mode="align",
    )
    page = max(attention.page_size_bytes, recurrent.page_size_bytes)
    attention = replace(attention, page_size_padded=page)
    recurrent = replace(recurrent, page_size_padded=page)
    names = [f"model.layers.{i}.cache" for i in range(6)]
    groups = [
        KVCacheGroupSpec(layer_names=names[::2], kv_cache_spec=attention),
        KVCacheGroupSpec(layer_names=names[1::2], kv_cache_spec=recurrent),
    ]
    packs = tuple(tuple(names[i : i + 2]) for i in range(0, 6, 2))
    plan = KVCacheConfig(
        num_blocks=3,
        kv_cache_groups=groups,
        kv_cache_tensors=[
            KVCacheTensor(size=3 * page, layers=list(pack), offset=0, layer_stride=0, block_stride=page)
            for pack in packs
        ],
    )
    plan.layerwise_cache_topology = LayerwiseCacheTopology(
        main_packs=packs, group_ids=tuple((name, i % 2) for i, name in enumerate(names))
    )
    config = SimpleNamespace(
        model_config=SimpleNamespace(get_num_layers=lambda _: 6),
        parallel_config=None,
        kv_transfer_config=SimpleNamespace(
            kv_connector="AscendStoreConnector",
            kv_connector_extra_config={
                "backend": "memcache",
                "use_layerwise": True,
                "layerwise_num_shared_buffers": buffers,
                "layerwise_independent_layers": [] if independent is None else independent,
            },
        ),
    )
    return plan, config


def test_declared_packs_need_no_model_tags_or_cache_name_conventions():
    plan, config = make_plan()
    apply_layerwise_kv_cache_plan(plan, config)
    layout = get_layerwise_reuse_layout(plan, 6, config.kv_transfer_config.kv_connector_extra_config)
    assert layout.buffer_slots == ((0, 1, 2, 3, 4, 5),)
    assert layout.prefetch_layer_map == {2: 0, 3: 1, 4: 2, 5: 3}
    assert len(plan.kv_cache_tensors) == 1
    assert plan.kv_cache_tensors[0].size == 3 * plan.kv_cache_tensors[0].block_stride


def test_independent_owner_keeps_ordinary_group_packing():
    plan, config = make_plan(independent=[0, 1])
    resident = deepcopy(plan.kv_cache_tensors[0])
    apply_layerwise_kv_cache_plan(plan, config)
    assert plan.kv_cache_tensors[0] == resident
    assert len(plan.kv_cache_tensors) == 2
    assert plan.layerwise_reuse_layout.prefetch_layer_map == {4: 2, 5: 3}


@pytest.mark.parametrize("independent,buffers", [("all", 1), ([], 3), ([0, 1], 2)])
def test_no_saving_retains_ordinary_descriptors(independent, buffers):
    plan, config = make_plan(buffers=buffers, independent=independent)
    original = deepcopy(plan.kv_cache_tensors)
    apply_layerwise_kv_cache_plan(plan, config)
    assert plan.kv_cache_tensors == original
    assert not plan.layerwise_reuse_layout.has_layer_reuse
    assert not getattr(plan, "layerwise_kv_cache_reuse", False)


def test_scheduler_projection_consumes_the_materialized_layout():
    plan, config = make_plan()
    apply_layerwise_kv_cache_plan(plan, config)
    projected = deepcopy(plan)
    projected.kv_cache_groups = []  # Scheduler projections need not retain owner specs.
    layout = get_layerwise_reuse_layout(projected, 6, {})
    assert layout.prefetch_layer_map == plan.layerwise_reuse_layout.prefetch_layer_map
    tensors = projected.kv_cache_tensors
    apply_layerwise_kv_cache_plan(projected, config)
    assert projected.kv_cache_tensors is tensors


def test_ordinary_pack_cannot_alias_owners_in_the_same_group():
    plan, config = make_plan()
    topology = plan.layerwise_cache_topology
    plan.layerwise_cache_topology = replace(topology, group_ids=tuple((name, 0) for name, _ in topology.group_ids))
    with pytest.raises(ValueError, match="actual scheduler cache groups"):
        apply_layerwise_kv_cache_plan(plan, config)


def test_recurrent_checkpoint_layout_remains_validated():
    plan, config = make_plan()
    plan.kv_cache_groups[1].kv_cache_spec = replace(
        plan.kv_cache_groups[1].kv_cache_spec, num_prefill_checkpoint_blocks=1
    )
    with pytest.raises(NotImplementedError, match="without extra prefill checkpoint blocks"):
        apply_layerwise_kv_cache_plan(plan, config)


def test_resident_auxiliary_views_cannot_create_fictitious_savings():
    plan, config = make_plan()
    auxiliary = [f"model.layers.{i}.auxiliary" for i in (0, 2, 4)]
    plan.kv_cache_groups.append(
        KVCacheGroupSpec(layer_names=auxiliary, kv_cache_spec=plan.kv_cache_groups[0].kv_cache_spec)
    )
    for tensor, name in zip(plan.kv_cache_tensors, auxiliary):
        tensor.layers.append(name)
    topology = plan.layerwise_cache_topology
    plan.layerwise_cache_topology = replace(
        topology, group_ids=topology.group_ids + tuple((name, 2) for name in auxiliary)
    )
    original = deepcopy(plan.kv_cache_tensors)
    apply_layerwise_kv_cache_plan(plan, config)
    assert plan.kv_cache_tensors == original
    assert not plan.layerwise_reuse_layout.has_layer_reuse


def test_declared_pack_cannot_combine_two_owners_of_the_same_group():
    plan, config = make_plan()
    topology = plan.layerwise_cache_topology
    names = [name for pack in topology.main_packs for name in pack]
    plan.layerwise_cache_topology = replace(
        topology, main_packs=((names[0], names[2]), (names[1], names[3]), (names[4], names[5]))
    )
    with pytest.raises(ValueError, match="disjoint cache groups"):
        apply_layerwise_kv_cache_plan(plan, config)
