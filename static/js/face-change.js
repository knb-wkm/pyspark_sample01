$(function(){
    svg_wrapper = $("#svgArea")
    svg_wrapper.load("/static/images/face_normal.svg svg");

    $("#to-negative").on("click", function(){
        svg_wrapper.load("/static/images/face_negative.svg svg");
    });

    $("#to-normal").on("click", function(){
        svg_wrapper.load("/static/images/face_normal.svg svg");
    });

    $("#to-positive").on("click", function(){
        svg_wrapper.load("/static/images/face_positive.svg svg");
    });


});
