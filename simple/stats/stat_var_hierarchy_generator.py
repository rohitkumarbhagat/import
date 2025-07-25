# Copyright 2024 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from dataclasses import dataclass
from dataclasses import replace
import hashlib
import json
import logging
import re
from typing import Self

from stats import schema_constants as sc
from stats.data import ParentSVG2ChildSpecializedNames
from stats.data import StatVarHierarchyResult
from stats.data import Triple
from stats.data import VerticalSpec

# The maximum length of a SVG ID is 255 characters to match the subject_id column length.
MAX_SVG_ID_LENGTH = 255
# The length of the hash suffix added to the SVG ID to avoid collisions.
SVG_ID_HASH_LENGTH = 8


def generate(triples: list[Triple], vertical_specs: list[VerticalSpec],
             dcid2name: dict[str, str]) -> StatVarHierarchyResult:
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a StatVarHierarchyResult object.
"""
  return _generate_internal(triples, vertical_specs, dcid2name).to_result()


def load_vertical_specs(data: str) -> list[VerticalSpec]:
  vertical_specs: list[VerticalSpec] = []

  for spec_json in json.loads(data).get("specs", []):
    vertical_specs.append(VerticalSpec.from_json(spec_json))

  return vertical_specs


# Helper functions and classes


# TODO: Pruning (e.g. ignore Thing).
def _generate_internal(triples: list[Triple],
                       vertical_specs: list[VerticalSpec],
                       dcid2name: dict[str, str]) -> "StatVarHierarchy":
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a list of output triples
representing the hierarchy.
"""

  # Extract SVs.
  svs = _extract_svs(triples)
  # Create SVGs.
  svgs = _create_all_svgs(svs, dcid2name)
  # Sort by SVG ID so it's easier to follow the hierarchy.
  svgs = dict(sorted(svgs.items()))

  # Get pop type svgs (they don't have a parent set at this stage).
  pop_type_svgs = _get_pop_type_svgs(svgs)
  # Attach verticals to pop type svgs.
  vertical_svgs = _attach_verticals(pop_type_svgs, vertical_specs, dcid2name)
  # Sort by SVG ID so it's easier to follow the verticals.
  vertical_svgs = dict(sorted(vertical_svgs.items()))

  # Combine all SVGs - verticals and hierarchy.
  final_svgs: dict[str, SVG] = {}
  # Add verticals.
  final_svgs.update(vertical_svgs)
  # Add hierarchy.
  final_svgs.update(svgs)

  # Generate SVG triples.
  svg_triples = _create_all_svg_triples(final_svgs)
  return StatVarHierarchy(svgs=final_svgs, svg_triples=svg_triples)


@dataclass(eq=True, frozen=True)
class PropVal:
  prop: str
  val: str

  def gen_pv_id(self) -> str:
    if self.val:
      return f"{_to_dcid_token(self.prop)}-{_to_dcid_token(self.val)}"
    return _to_dcid_token(self.prop)

  def gen_pv_name(self, dcid2name: dict[str, str]) -> str:
    if self.val:
      return f"{_gen_name(self.prop, dcid2name)} = {_gen_name(self.val, dcid2name)}"
    return _gen_name(self.prop, dcid2name)


# TODO: DPV handling.
@dataclass
class SVPropVals:
  sv_id: str
  population_type: str
  # The PVs here are ordered by prop.
  # They are originally ordered in the extract_svs method
  # and maintain the order thereafter.
  pvs: list[PropVal]
  measured_property: str

  def gen_svg_id(self):
    svg_id = f"{sc.CUSTOM_SVG_PREFIX}{_to_dcid_token(self.population_type)}"
    for pv in self.pvs:
      svg_id = f"{svg_id}_{pv.gen_pv_id()}"
    # SVG IDs cannot exceed the maximum length of the subject_id column.
    # If the SVG ID exceeds the maximum length, truncate it and add a hash suffix.
    if len(svg_id) > MAX_SVG_ID_LENGTH:
      # Get SHA-256 hash of full svg_id
      hash_suffix = hashlib.sha256(
          svg_id.encode()).hexdigest()[:SVG_ID_HASH_LENGTH]

      # Truncate svg_id to leave room for hash suffix and dash.
      max_base_length = MAX_SVG_ID_LENGTH - SVG_ID_HASH_LENGTH - 1  # -1 for dash
      svg_id = f"{svg_id[:max_base_length]}-{hash_suffix}"
    return svg_id

  def gen_svg_name(self, dcid2name: dict[str, str]):
    svg_name = _gen_name(self.population_type, dcid2name)
    if self.pvs:
      pvs_str = ", ".join(map(lambda pv: pv.gen_pv_name(dcid2name), self.pvs))
      svg_name = f"{svg_name} With {pvs_str}"
    return svg_name

  def gen_specialized_name(self, parent_pvs: Self, dcid2name: dict[str,
                                                                   str]) -> str:
    parent_parts = parent_pvs._get_pv_parts()
    child_parts = self._get_pv_parts()
    parts = [part for part in child_parts if part not in parent_parts]
    return ", ".join(map(lambda part: _gen_name(part, dcid2name), parts))

  # Creates and returns a new SVPropVals object with the same fields as this object
  # except for PVs which are set to the specified list.
  def with_pvs(self, pvs: list[PropVal]) -> Self:
    return replace(self, pvs=pvs)

  # Returns an ordered set of PVs as a dict (since sets don't maintain order).
  def _get_pv_parts(self) -> dict[str, bool]:
    parts: dict[str, bool] = {}
    for pv in self.pvs:
      if pv.prop:
        parts[pv.prop] = True
      if pv.val:
        parts[pv.val] = True
    return parts


class SVG:

  def __init__(self, svg_id: str, svg_name: str) -> None:
    self.svg_id = svg_id
    self.svg_name = svg_name

    # Using dict instead of sets below to maintain insertion order.
    # Maintaining order maintains results consistency and helps with tests.
    self.sv_ids: dict[str, bool] = {}
    self.parent_svg_ids: dict[str, bool] = {}
    self.child_svg_id_2_specialized_name: dict[str, str] = {}
    self.measured_properties: dict[str, bool] = {}

    self.parent_svgs_processed: bool = False
    # Only relevant for PV hierarchy.
    # Indicates whether this SVG is for PVs where
    # one of the PVs does not have a value.
    self.has_prop_without_val: bool = False
    self.sample_sv: SVPropVals | None = None

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []

    # SVG info
    triples.append(
        Triple(self.svg_id,
               sc.PREDICATE_TYPE_OF,
               object_id=sc.TYPE_STATISTICAL_VARIABLE_GROUP))
    triples.append(
        Triple(self.svg_id, sc.PREDICATE_NAME, object_value=self.svg_name))

    # SVG parents ("specializationOf")
    for parent_svg_id in self.parent_svg_ids.keys():
      triples.append(
          Triple(self.svg_id,
                 sc.PREDICATE_SPECIALIZATION_OF,
                 object_id=parent_svg_id))

    # SV members ("memberOf")
    for sv_id in self.sv_ids:
      triples.append(
          Triple(sv_id, sc.PREDICATE_MEMBER_OF, object_id=self.svg_id))

    return triples

  def gen_specialized_name(self, parent_svg: Self, dcid2name: dict[str,
                                                                   str]) -> str:
    if self.sample_sv and parent_svg.sample_sv:
      return self.sample_sv.gen_specialized_name(parent_svg.sample_sv,
                                                 dcid2name)
    return ""

  # For testing.
  def json(self) -> dict:
    return {
        "svg_id": self.svg_id,
        "svg_name": self.svg_name,
        "sv_ids": list(self.sv_ids.keys()),
        "parent_svg_ids": list(self.parent_svg_ids.keys()),
        "child_svg_specialized_names": self.child_svg_id_2_specialized_name,
        "mprops": sorted(list(self.measured_properties.keys()))
    }

  def __str__(self) -> str:
    return json.dumps(self.json(), indent=1)


@dataclass
class StatVarHierarchy:
  # Dict from SVG dcid to SVG.
  svgs: dict[str, SVG]
  svg_triples: list[Triple]

  def to_result(self) -> StatVarHierarchyResult:
    return StatVarHierarchyResult(self.svg_triples,
                                  self._get_svg_specialized_names())

  def _get_svg_specialized_names(self) -> ParentSVG2ChildSpecializedNames:
    specialized_names: ParentSVG2ChildSpecializedNames = {}
    for svg_id, svg in self.svgs.items():
      specialized_names[svg_id] = svg.child_svg_id_2_specialized_name
    return specialized_names


# Attaches matching pop type svgs to vertical svgs, creates those vertical svgs and returns them.
def _attach_verticals(poptype2svg: dict[str, SVG],
                      vertical_specs: list[VerticalSpec],
                      dcid2name: dict[str, str]) -> dict[str, SVG]:
  vertical_svgs: dict[str, SVG] = {}
  for vertical_spec in vertical_specs:
    pop_type_svg = poptype2svg.get(vertical_spec.population_type)
    # If no matching pop type svg, skip.
    if not pop_type_svg:
      continue
    # If no vertical spec mprop in svg mprops, skip.
    if not (vertical_spec.measured_properties &
            pop_type_svg.measured_properties.keys()):
      continue
    # Put pop type svg under all verticals in the spec.
    for vertical in vertical_spec.verticals:
      vertical_svg = _get_or_create_vertical_svg(vertical, vertical_svgs,
                                                 dcid2name)
      vertical_svgs[vertical_svg.svg_id] = vertical_svg
      vertical_svg.child_svg_id_2_specialized_name[pop_type_svg.svg_id] = ""
      pop_type_svg.parent_svg_ids[vertical_svg.svg_id] = True
      for mprop in vertical_spec.measured_properties:
        vertical_svg.measured_properties[mprop] = True

  # Any pop type svgs that were not attached to a vertical (i.e. with no parent)
  # should be put under custom dc root.
  for svg in poptype2svg.values():
    if not svg.parent_svg_ids:
      svg.parent_svg_ids[sc.DEFAULT_CUSTOM_ROOT_SVG_ID] = True
  return vertical_svgs


def _get_or_create_vertical_svg(vertical: str, vertical_svgs: dict[str, SVG],
                                dcid2name: dict[str, str]) -> SVG:
  vertical_svg_id = f"{sc.CUSTOM_SVG_PREFIX}{vertical}"
  vertical_svg = vertical_svgs.get(vertical_svg_id)
  if not vertical_svg:
    vertical_svg = SVG(vertical_svg_id, _gen_name(vertical, dcid2name))
    vertical_svg.parent_svg_ids[sc.DEFAULT_CUSTOM_ROOT_SVG_ID] = True
  return vertical_svg


# Returns a dict from population type to SVG.
def _get_pop_type_svgs(svgs: dict[str, SVG]) -> dict[str, SVG]:
  poptype2svg: dict[str, SVG] = {}
  for svg in svgs.values():
    if not svg.parent_svg_ids and (svg.sample_sv and
                                   svg.sample_sv.population_type):
      poptype2svg[svg.sample_sv.population_type] = svg
  return poptype2svg


def _get_or_create_svg(svgs: dict[str, SVG], sv: SVPropVals,
                       dcid2name: dict[str, str]) -> SVG:
  svg_id = sv.gen_svg_id()
  svg = svgs.get(svg_id)
  if not svg:
    svg = SVG(svg_id=svg_id, svg_name=sv.gen_svg_name(dcid2name))
    svg.sample_sv = sv
    svgs[svg_id] = svg
  # Add SV mprop to the SVG.
  if sv.measured_property:
    svg.measured_properties[sv.measured_property] = True
  return svg


def _create_all_svg_triples(svgs: dict[str, SVG]):
  triples: list[Triple] = []
  for svg in svgs.values():
    triples.extend(svg.triples())
  return triples


def _create_all_svgs(svs: list[SVPropVals],
                     dcid2name: dict[str, str]) -> dict[str, SVG]:
  svgs = _create_leaf_svgs(svs, dcid2name)
  for svg_id in list(svgs.keys()):
    _create_parent_svgs(svg_id, svgs, dcid2name)
  return svgs


# Create SVGs that the SVs are directly attached to.
def _create_leaf_svgs(svs: list[SVPropVals],
                      dcid2name: dict[str, str]) -> dict[str, SVG]:
  svgs: dict[str, SVG] = {}
  for sv in svs:
    svg = _get_or_create_svg(svgs, sv, dcid2name)
    # Insert SV into SVG.
    svg.sv_ids[sv.sv_id] = True
  return svgs


def _add_measured_properties_to_parent_svgs(mprops: dict[str, bool],
                                            parent_svg_ids: dict[str, bool],
                                            svgs: dict[str, SVG]):
  if not mprops or not parent_svg_ids:
    return
  for parent_svg_id in parent_svg_ids:
    parent_svg = svgs[parent_svg_id]
    parent_svg.measured_properties.update(mprops)
    _add_measured_properties_to_parent_svgs(mprops, parent_svg.parent_svg_ids,
                                            svgs)


def _create_parent_svg(parent_sv: SVPropVals, svg: SVG, svgs: dict[str, SVG],
                       svg_has_prop_without_val: bool, dcid2name: dict[str,
                                                                       str]):
  parent_svg = _get_or_create_svg(svgs, parent_sv, dcid2name)

  # Add parent child relationships.
  svg.parent_svg_ids[parent_svg.svg_id] = True
  parent_svg.child_svg_id_2_specialized_name[
      svg.svg_id] = svg.gen_specialized_name(parent_svg, dcid2name)

  # Add child mprops to all parents recursively.
  _add_measured_properties_to_parent_svgs(svg.measured_properties,
                                          svg.parent_svg_ids, svgs)

  if not parent_svg.parent_svgs_processed:
    parent_svg.has_prop_without_val = svg_has_prop_without_val
    _create_parent_svgs(parent_svg.svg_id, svgs, dcid2name)


def _create_parent_svgs(svg_id: str, svgs: dict[str, SVG],
                        dcid2name: dict[str, str]):
  svg = svgs[svg_id]
  sv = svg.sample_sv

  # If no PVs left, we've reached the top of the population type hierarchy and we simply return.
  # We'll attach it to verticals or the root upstream.
  if not sv.pvs:
    return

  # Process SVGs without a val
  # e.g. The SVG c/g/Person_Gender_Race-Asian represents a SVG for
  # persons of all genders with race = Asian. In this case,
  # the prop gender does not have a val.
  if svg.has_prop_without_val:
    parent_pvs: list[PropVal] = []
    for pv in sv.pvs:
      # Skip prop without val.
      if not pv.val:
        continue
      else:
        parent_pvs.append(pv)
    _create_parent_svg(parent_sv=sv.with_pvs(parent_pvs),
                       svg=svg,
                       svgs=svgs,
                       svg_has_prop_without_val=False,
                       dcid2name=dcid2name)
  # Process SVGs with vals.
  else:
    for pv1 in sv.pvs:
      parent_pvs: list[PropVal] = []
      for pv2 in sv.pvs:
        if pv1.prop == pv2.prop:
          # Remove val of one property at a time.
          parent_pvs.append(PropVal(pv2.prop, ""))
        else:
          parent_pvs.append(pv2)
      # Create parent SVG for each combination.
      _create_parent_svg(parent_sv=sv.with_pvs(parent_pvs),
                         svg=svg,
                         svgs=svgs,
                         svg_has_prop_without_val=True,
                         dcid2name=dcid2name)

  svg.parent_svgs_processed = True


def _split_camel_case(s: str) -> str:
  """Splits camel case strings into separate words with spaces.

  e.g. "CamelCaseString" => "Camel Case String"
  """
  return re.sub(r"([A-Z])", r" \1", s).strip()


# s.capitalize() turns "energySource" into "Energysource" instead of "EnergySource"
# hence this method.
def _capitalize(s: str) -> str:
  if not s:
    return s
  return s[0].upper() + s[1:]


# Capitalizes the first letter and then splits any camel case strings.
# e.g. "energySource" -> "EnergySource" -> "Energy Source"
def _capitalize_and_split(s: str) -> str:
  return _split_camel_case(_capitalize(s))


def _gen_name(dcid: str, dcid2name: dict[str, str]) -> str:
  return _capitalize_and_split(dcid2name.get(dcid) or dcid)


def _to_dcid_token(token: str) -> str:
  # Remove all non-alphanumeric characters.
  result = re.sub("[^0-9a-zA-Z]+", "", token)
  return _capitalize(result)


def _extract_svs(triples: list[Triple]) -> list[SVPropVals]:
  """Extracts SVs from the input triples.
  The following SV properties used for generating the SV hierarchy are extracted:
  - dcid
  - population type
  - PVs not in SV_HIERARCHY_PROPS_BLOCKLIST
  - measured property
  """

  # Using dict instead of set to maintain order.
  # Maintaining order maintains results consistency and helps with tests.
  sv_ids: dict[str, bool] = {}

  dcid2poptype: dict[str, str] = {}
  dcid2pvs: dict[str, dict[str, str]] = {}
  dcid2mprop: dict[str, str] = {}

  for triple in triples:
    value = triple.object_id or triple.object_value
    if not value:
      logging.warning("Skipping, no value found for triple (%s).", str(triple))
      continue

    if triple.predicate == sc.PREDICATE_TYPE_OF:
      if value == sc.TYPE_STATISTICAL_VARIABLE:
        sv_ids[triple.subject_id] = True
    elif triple.predicate == sc.PREDICATE_POPULATION_TYPE:
      dcid2poptype[triple.subject_id] = value
    elif triple.predicate == sc.PREDICATE_MEASURED_PROPERTY:
      dcid2mprop[triple.subject_id] = value
    elif triple.predicate not in sc.SV_HIERARCHY_PROPS_BLOCKLIST:
      pvs = dcid2pvs.setdefault(triple.subject_id, {})
      pvs[triple.predicate] = value

  svs = []
  # Filter and populate SVs.
  for sv_id in sv_ids.keys():
    pop_type = dcid2poptype.get(sv_id, sc.DEFAULT_POPULATION_TYPE)
    prop_vals: list[PropVal] = []
    mprop = dcid2mprop.get(sv_id, "")
    # Sort prop vals by keys since we use this order to generate SVG IDs later.
    for (p, v) in sorted(dcid2pvs.get(sv_id, {}).items()):
      prop_vals.append(PropVal(p, v))
    svs.append(SVPropVals(sv_id, pop_type, prop_vals, mprop))

  return svs
