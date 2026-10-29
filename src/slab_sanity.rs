use crate::page::Page;
use crate::page_container_layer::PageSanityTrait;
use crate::page_container_layer::ReadPageError;

// A Slab is a continous series of pages. These slabs
// could be used for different things so checking
// their sanity will depend on what the slab is being
// used for.
//
// The page_container_layer is reposnsible for checking
// page sanity and doing any page repairs - it does
// not understand slabs so we need a way of working
// with it to check slab sanity.
//
// Interface for checking the sanity of the slab,
// this function is passed to the page_container_layer
// to allow it to do sanity for the slab. The
// page_container_layer supplies PageSanityTrait which
// will sanity check an individual page and repair the
// pages if it needs it and it can do the repaier (ie
// has a mirror).
//
pub trait SlabSanityTrait {
    // The input, slab: &mut Page should be treated
    // as a set of raw bytes read from disk.
    // TODO - should consume slab: &mut Page and
    // return a Slab object.
    // The check_slab_sanity is provided with
    // PageSanityTrait which it should use to
    // check individual pages int the slab -
    // the page_container_layer will provide this
    // function and will check and repair individual
    // pages.
    fn check_slab_sanity(
        &self,
        slab: &mut Page,
        page_sanity: &mut dyn PageSanityTrait,
    ) -> Result<(), ReadPageError>;
}

pub struct LogSlabSanity {}

impl SlabSanityTrait for LogSlabSanity {
    fn check_slab_sanity(
        &self,
        _slab: &mut Page,
        _page_sanity: &mut dyn PageSanityTrait,
    ) -> Result<(), ReadPageError> {
        Ok(())
    }
}
